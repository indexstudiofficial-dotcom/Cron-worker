/**
 * Reportli Planner Cron Worker
 * Version: 2.0.0
 *
 * PURPOSE:
 * Runs every 2 minutes.
 *
 * WORKFLOW:
 * 1. Fetch applications from Supabase.
 * 2. Check the latest Reddit task for each application.
 * 3. Check the latest Apollo task for each application.
 * 4. Create a task if no task exists.
 * 5. Create a new task if the latest task has pending_count = 0.
 * 6. Skip task creation while the latest task is still pending.
 *
 * REQUIRED SECRETS:
 * SUPABASE_URL
 * SUPABASE_SERVICE_ROLE_KEY
 *
 * EXISTING PLANNER WORKER:
 * https://reportli-ai-planner.reportliaihq.workers.dev/create-task
 */

// ============================================================
// CONFIGURATION
// ============================================================

const PLANNER_CREATE_TASK_URL =
  "https://reportli-ai-planner.reportliaihq.workers.dev/create-task";

const TASK_SOURCES = ["reddit", "apollo"];

const MAX_APPLICATIONS = 1000;

const REQUEST_TIMEOUT_MS = 20000;


// ============================================================
// MAIN WORKER EXPORT
// IMPORTANT: Cloudflare requires the scheduled() handler.
// ============================================================

export default {
  // ----------------------------------------------------------
  // HTTP HANDLER
  // GET /health  -> Health check
  // POST /run    -> Manually execute the cron workflow
  // ----------------------------------------------------------

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health" && request.method === "GET") {
      return Response.json({
        success: true,
        worker: "reportli-planner-cron",
        message: "Worker is running",
        cron_schedule: "*/2 * * * *",
        timestamp: new Date().toISOString(),
      });
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const result = await runCron(env);

        return Response.json(result, {
          status: result.errors.length > 0 ? 207 : 200,
        });
      } catch (error) {
        console.error("Manual run failed:", error.message);

        return Response.json(
          {
            success: false,
            error: error.message,
          },
          { status: 500 }
        );
      }
    }

    return Response.json(
      {
        success: false,
        error: "Route not found.",
        available_routes: [
          "GET /health",
          "POST /run",
        ],
      },
      { status: 404 }
    );
  },

  // ----------------------------------------------------------
  // SCHEDULED HANDLER
  // This fixes:
  // "Handler does not export a scheduled() function"
  // ----------------------------------------------------------

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(
      runCron(env).catch((error) => {
        console.error(
          "Scheduled cron execution failed:",
          error.message
        );
      })
    );
  },
};


// ============================================================
// CRON WORKFLOW
// ============================================================

async function runCron(env) {
  validateEnvironment(env);

  const startedAt = new Date().toISOString();

  console.log("Reportli planner cron started:", startedAt);

  const result = {
    success: true,
    started_at: startedAt,
    applications_checked: 0,
    tasks_checked: 0,
    tasks_created: 0,
    tasks_skipped: 0,
    errors: [],
  };

  // ----------------------------------------------------------
  // STEP 1: Fetch applications
  // ----------------------------------------------------------

  const applications = await getApplications(env);

  result.applications_checked = applications.length;

  console.log(
    `Found ${applications.length} applications`
  );

  // ----------------------------------------------------------
  // STEP 2: Check Reddit and Apollo tasks
  // ----------------------------------------------------------

  for (const application of applications) {
    const applicationId = application.id;

    if (!applicationId) {
      result.errors.push({
        application_id: null,
        error: "Application is missing its id",
      });

      continue;
    }

    for (const source of TASK_SOURCES) {
      result.tasks_checked++;

      try {
        console.log(
          `Checking application ${applicationId}, source ${source}`
        );

        // Find the latest task for this application and source.
        const latestTask = await getLatestTask(
          env,
          applicationId,
          source
        );

        // ----------------------------------------------------
        // CASE 1: No task exists
        // Create the first task.
        // ----------------------------------------------------

        if (!latestTask) {
          console.log(
            `No ${source} task found for ${applicationId}`
          );

          await createPlannerTask(
            applicationId,
            source
          );

          result.tasks_created++;

          continue;
        }

        // ----------------------------------------------------
        // CASE 2: Latest task has pending_count = 0
        // Create the next task.
        // ----------------------------------------------------

        const pendingCount = Number(
          latestTask.pending_count
        );

        if (
          latestTask.pending_count !== null &&
          latestTask.pending_count !== undefined &&
          Number.isFinite(pendingCount) &&
          pendingCount === 0
        ) {
          console.log(
            `Latest ${source} task is complete for ${applicationId}`
          );

          await createPlannerTask(
            applicationId,
            source
          );

          result.tasks_created++;

          continue;
        }

        // ----------------------------------------------------
        // CASE 3: Task is still pending, or its count is unknown
        // Do not create a duplicate task.
        // ----------------------------------------------------

        console.log(
          `Skipping ${source} for ${applicationId}. ` +
          `pending_count=${latestTask.pending_count}`
        );

        result.tasks_skipped++;

      } catch (error) {
        console.error(
          `Task processing failed for application ${applicationId}, ` +
          `source ${source}:`,
          error.message
        );

        result.errors.push({
          application_id: String(applicationId),
          source,
          error: error.message,
        });
      }
    }
  }

  // ----------------------------------------------------------
  // STEP 3: Finish
  // ----------------------------------------------------------

  result.finished_at = new Date().toISOString();
  result.success = result.errors.length === 0;

  console.log(
    "Reportli planner cron finished:",
    JSON.stringify(result)
  );

  return result;
}


// ============================================================
// ENVIRONMENT VALIDATION
// ============================================================

function validateEnvironment(env) {
  if (!env.SUPABASE_URL) {
    throw new Error(
      "Missing SUPABASE_URL secret"
    );
  }

  if (!env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error(
      "Missing SUPABASE_SERVICE_ROLE_KEY secret"
    );
  }
}


// ============================================================
// SUPABASE REQUEST HELPER
// ============================================================

async function supabaseRequest(env, path) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const url = `${baseUrl}/rest/v1/${path}`;

  const response = await fetchWithTimeout(
    url,
    {
      method: "GET",
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization:
          `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        Accept: "application/json",
      },
    }
  );

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Supabase request failed (${response.status}): ` +
      responseText
    );
  }

  try {
    return responseText
      ? JSON.parse(responseText)
      : [];
  } catch {
    throw new Error(
      "Supabase returned invalid JSON"
    );
  }
}


// ============================================================
// FETCH APPLICATIONS
//
// Expected table:
// applications
//
// Expected column:
// id
// ============================================================

async function getApplications(env) {
  const query = new URLSearchParams({
    select: "id",
    order: "id.asc",
    limit: String(MAX_APPLICATIONS),
  });

  const applications = await supabaseRequest(
    env,
    `applications?${query.toString()}`
  );

  if (!Array.isArray(applications)) {
    throw new Error(
      "Supabase applications response is not an array"
    );
  }

  return applications;
}


// ============================================================
// GET LATEST TASK
//
// Expected table:
// planner_runs
//
// Expected columns:
// application_id
// source
// pending_count
// created_at
// ============================================================

async function getLatestTask(
  env,
  applicationId,
  source
) {
  const query = new URLSearchParams({
    select:
      "id,application_id,source,pending_count,created_at",

    application_id: `eq.${applicationId}`,

    source: `eq.${source}`,

    order: "created_at.desc",

    limit: "1",
  });

  const rows = await supabaseRequest(
    env,
    `planner_runs?${query.toString()}`
  );

  if (!Array.isArray(rows)) {
    throw new Error(
      "Supabase planner_runs response is not an array"
    );
  }

  return rows.length > 0 ? rows[0] : null;
}


// ============================================================
// CREATE TASK THROUGH THE EXISTING PLANNER WORKER
//
// The planner worker must accept:
// POST /create-task
//
// JSON body:
// {
//   "application_id": "your-application-id",
//   "source": "reddit"
// }
//
// Or:
// {
//   "application_id": "your-application-id",
//   "source": "apollo"
// }
// ============================================================

async function createPlannerTask(
  applicationId,
  source
) {
  if (!TASK_SOURCES.includes(source)) {
    throw new Error(
      `Invalid task source: ${source}`
    );
  }

  const payload = {
    application_id: String(applicationId),
    source,
  };

  // Convert the payload into valid JSON exactly once.
  const requestBody = JSON.stringify(payload);

  console.log(
    "Sending task request:",
    requestBody
  );

  const response = await fetchWithTimeout(
    PLANNER_CREATE_TASK_URL,
    {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },

      body: requestBody,
    }
  );

  const responseText = await response.text();

  console.log(
    `Planner worker returned HTTP ${response.status}`
  );

  console.log(
    "Planner worker response:",
    responseText.slice(0, 2000)
  );

  if (!response.ok) {
    throw new Error(
      `Planner worker returned HTTP ${response.status}: ` +
      responseText
    );
  }

  // Verify that the endpoint returned JSON.
  let responseData;

  try {
    responseData = JSON.parse(responseText);
  } catch {
    throw new Error(
      "Planner worker returned a non-JSON response: " +
      responseText.slice(0, 500)
    );
  }

  // Some APIs return HTTP 200 with success: false.
  if (responseData.success === false) {
    throw new Error(
      responseData.error ||
      "Planner worker reported that task creation failed"
    );
  }

  console.log(
    `Task creation request completed for ${applicationId}, ${source}`
  );

  return responseData;
}


// ============================================================
// FETCH WITH TIMEOUT
// ============================================================

async function fetchWithTimeout(
  url,
  options = {}
) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    REQUEST_TIMEOUT_MS
  );

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(
        `Request timed out after ${REQUEST_TIMEOUT_MS} ms`
      );
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}
