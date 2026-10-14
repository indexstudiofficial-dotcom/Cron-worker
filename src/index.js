/**
 * Reportli Planner Cron Worker
 * Version: 1.0.0
 *
 * PURPOSE:
 * Runs every 2 minutes and checks planner_runs.
 *
 * RULES:
 * 1. If an application has no Reddit task, create one.
 * 2. If an application has no Apollo task, create one.
 * 3. If the latest Reddit task has pending_count = 0, create another.
 * 4. If the latest Apollo task has pending_count = 0, create another.
 *
 * TARGET WORKER:
 * https://reportli-ai-planner.reportliaihq.workers.dev/create-task
 *
 * REQUIRED SECRETS:
 * SUPABASE_URL
 * SUPABASE_SERVICE_ROLE_KEY
 */

const VERSION = "1.0.0";

const PLANNER_CREATE_TASK_URL =
  "https://reportli-ai-planner.reportliaihq.workers.dev/create-task";

const SOURCES = ["reddit", "apollo"];

// Limit concurrent requests to avoid overwhelming the planner worker.
const MAX_CONCURRENT_REQUESTS = 3;

// ------------------------------------------------------
// MAIN WORKER
// ------------------------------------------------------

export default {
  /**
   * Cron trigger:
   * Every 2 minutes.
   */
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(runCron(env));
  },

  /**
   * HTTP routes for testing.
   */
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(),
      });
    }

    if (url.pathname === "/health") {
      return jsonResponse({
        success: true,
        worker: "reportli-planner-cron",
        version: VERSION,
        message: "Cron worker is running.",
      });
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const result = await runCron(env);

        return jsonResponse({
          success: true,
          worker: "reportli-planner-cron",
          version: VERSION,
          result,
        });
      } catch (error) {
        console.error("Manual cron run failed:", error);

        return jsonResponse(
          {
            success: false,
            error: error.message || "Cron run failed.",
          },
          500
        );
      }
    }

    return jsonResponse(
      {
        success: false,
        error: "Route not found.",
        available_routes: [
          "GET /health",
          "POST /run",
        ],
      },
      404
    );
  },
};

// ------------------------------------------------------
// CRON LOGIC
// ------------------------------------------------------

async function runCron(env) {
  validateEnvironment(env);

  const startedAt = new Date().toISOString();

  console.log(`[${VERSION}] Cron started at ${startedAt}`);

  const applications = await getApplications(env);

  console.log(`Found ${applications.length} applications.`);

  const summary = {
    started_at: startedAt,
    applications_found: applications.length,
    tasks_checked: 0,
    tasks_created: 0,
    tasks_skipped: 0,
    errors: [],
    results: [],
  };

  // Read the newest task for every application and source.
  const latestTasks = await getLatestTasksForApplications(
    env,
    applications
  );

  // Build a list of tasks that need to be created.
  const tasksToCreate = [];

  for (const application of applications) {
    const applicationId = application.id;

    if (!applicationId) {
      continue;
    }

    for (const source of SOURCES) {
      summary.tasks_checked++;

      const key = makeTaskKey(applicationId, source);

      const latestTask = latestTasks.get(key);

      // CASE 1:
      // No task exists for this application and source.
      if (!latestTask) {
        tasksToCreate.push({
          application_id: applicationId,
          source,
          reason: "missing_task",
        });

        continue;
      }

      // CASE 2:
      // The latest task has completed its pending work.
      if (Number(latestTask.pending_count) === 0) {
        tasksToCreate.push({
          application_id: applicationId,
          source,
          reason: "pending_count_zero",
          previous_task_id: latestTask.id || null,
        });

        continue;
      }

      // CASE 3:
      // A task already exists and still has pending work.
      summary.tasks_skipped++;

      console.log(
        `Skipping ${source} for application ${applicationId}: ` +
        `latest pending_count=${latestTask.pending_count}`
      );
    }
  }

  console.log(
    `Tasks requiring creation: ${tasksToCreate.length}`
  );

  // Process task creation with limited concurrency.
  const taskResults = await runWithConcurrency(
    tasksToCreate,
    MAX_CONCURRENT_REQUESTS,
    async (task) => {
      try {
        const result = await createPlannerTask(env, task);

        summary.tasks_created++;

        const entry = {
          application_id: task.application_id,
          source: task.source,
          reason: task.reason,
          success: true,
          response: result,
        };

        summary.results.push(entry);

        return entry;
      } catch (error) {
        const entry = {
          application_id: task.application_id,
          source: task.source,
          reason: task.reason,
          success: false,
          error: error.message || "Unknown error",
        };

        summary.errors.push(entry);
        summary.results.push(entry);

        console.error(
          `Failed to create ${task.source} task for ` +
          `${task.application_id}:`,
          error
        );

        return entry;
      }
    }
  );

  summary.finished_at = new Date().toISOString();
  summary.task_results_count = taskResults.length;

  console.log(
    "Cron finished:",
    JSON.stringify({
      applications_found: summary.applications_found,
      tasks_checked: summary.tasks_checked,
      tasks_created: summary.tasks_created,
      tasks_skipped: summary.tasks_skipped,
      errors: summary.errors.length,
    })
  );

  return summary;
}

// ------------------------------------------------------
// GET APPLICATIONS
// ------------------------------------------------------

async function getApplications(env) {
  const url = new URL(
    `${env.SUPABASE_URL}/rest/v1/applications`
  );

  url.searchParams.set("select", "id,user_id");
  url.searchParams.set("order", "id.asc");
  url.searchParams.set("limit", "1000");

  const response = await supabaseFetch(env, url.toString(), {
    method: "GET",
  });

  if (!response.ok) {
    const body = await response.text();

    throw new Error(
      `Failed to fetch applications. ` +
      `HTTP ${response.status}: ${body}`
    );
  }

  const data = await response.json();

  if (!Array.isArray(data)) {
    throw new Error(
      "Unexpected applications response from Supabase."
    );
  }

  return data;
}

// ------------------------------------------------------
// GET THE LATEST TASK FOR EACH APPLICATION AND SOURCE
// ------------------------------------------------------

async function getLatestTasksForApplications(
  env,
  applications
) {
  const latestTasks = new Map();

  /*
   * We query planner_runs separately for each application.
   *
   * Each query gets the newest row for each source.
   *
   * This prevents an old completed row from repeatedly
   * triggering new tasks when a newer task is pending.
   */

  const lookups = [];

  for (const application of applications) {
    for (const source of SOURCES) {
      lookups.push({
        application_id: application.id,
        source,
      });
    }
  }

  await runWithConcurrency(
    lookups,
    MAX_CONCURRENT_REQUESTS,
    async (lookup) => {
      const url = new URL(
        `${env.SUPABASE_URL}/rest/v1/planner_runs`
      );

      url.searchParams.set(
        "select",
        "id,application_id,source,pending_count,created_at"
      );

      url.searchParams.set(
        "application_id",
        `eq.${lookup.application_id}`
      );

      url.searchParams.set(
        "source",
        `eq.${lookup.source}`
      );

      url.searchParams.set(
        "order",
        "created_at.desc"
      );

      url.searchParams.set("limit", "1");

      const response = await supabaseFetch(
        env,
        url.toString(),
        {
          method: "GET",
        }
      );

      if (!response.ok) {
        const body = await response.text();

        throw new Error(
          `Failed to read planner_runs for application ` +
          `${lookup.application_id}, source ${lookup.source}. ` +
          `HTTP ${response.status}: ${body}`
        );
      }

      const rows = await response.json();

      const key = makeTaskKey(
        lookup.application_id,
        lookup.source
      );

      if (Array.isArray(rows) && rows.length > 0) {
        latestTasks.set(key, rows[0]);
      }
    }
  );

  return latestTasks;
}

// ------------------------------------------------------
// CALL THE EXISTING PLANNER WORKER
// ------------------------------------------------------

async function createPlannerTask(env, task) {
  const payload = {
    application_id: task.application_id,
    source: task.source,
  };

  console.log(
    `Creating ${task.source} task for application ` +
    `${task.application_id}. Reason: ${task.reason}`
  );

  const response = await fetch(PLANNER_CREATE_TASK_URL, {
    method: "POST",

    headers: {
      "Content-Type": "application/json",
    },

    body: JSON.stringify(payload),
  });

  const responseText = await response.text();

  let responseData;

  try {
    responseData = responseText
      ? JSON.parse(responseText)
      : {};
  } catch {
    responseData = {
      raw_response: responseText,
    };
  }

  if (!response.ok) {
    throw new Error(
      `Planner worker returned HTTP ${response.status}: ` +
      JSON.stringify(responseData)
    );
  }

  /*
   * Do not assume HTTP 200 means the task was created.
   * Check the JSON response for an explicit failure.
   */
  if (
    responseData &&
    typeof responseData === "object" &&
    responseData.success === false
  ) {
    throw new Error(
      `Planner worker reported failure: ` +
      JSON.stringify(responseData)
    );
  }

  console.log(
    `Planner worker response for ${task.source}:`,
    JSON.stringify(responseData)
  );

  return responseData;
}

// ------------------------------------------------------
// SUPABASE REST HELPER
// ------------------------------------------------------

async function supabaseFetch(env, url, options = {}) {
  const headers = new Headers(options.headers || {});

  headers.set(
    "apikey",
    env.SUPABASE_SERVICE_ROLE_KEY
  );

  headers.set(
    "Authorization",
    `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  );

  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  return fetch(url, {
    ...options,
    headers,
  });
}

// ------------------------------------------------------
// CONCURRENCY HELPER
// ------------------------------------------------------

async function runWithConcurrency(
  items,
  concurrency,
  callback
) {
  const results = new Array(items.length);

  let nextIndex = 0;

  const workerCount = Math.min(
    Math.max(1, concurrency),
    items.length
  );

  const workers = Array.from(
    { length: workerCount },
    async () => {
      while (true) {
        const index = nextIndex++;

        if (index >= items.length) {
          return;
        }

        try {
          results[index] = await callback(items[index]);
        } catch (error) {
          /*
           * Database lookup errors must not be silently
           * treated as missing tasks. Re-throw them.
           */
          throw error;
        }
      }
    }
  );

  await Promise.all(workers);

  return results;
}

// ------------------------------------------------------
// UTILITIES
// ------------------------------------------------------

function makeTaskKey(applicationId, source) {
  return `${String(applicationId)}::${String(source)}`;
}

function validateEnvironment(env) {
  const missing = [];

  if (!env.SUPABASE_URL) {
    missing.push("SUPABASE_URL");
  }

  if (!env.SUPABASE_SERVICE_ROLE_KEY) {
    missing.push("SUPABASE_SERVICE_ROLE_KEY");
  }

  if (missing.length > 0) {
    throw new Error(
      `Missing Cloudflare secrets: ${missing.join(", ")}`
    );
  }
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      ...corsHeaders(),
      "Content-Type": "application/json; charset=utf-8",
    },
  });
  }
