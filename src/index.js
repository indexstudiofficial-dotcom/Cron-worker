/**
 * Reportli Planner Cron Worker
 * Version: 3.0.0
 *
 * PURPOSE:
 * Runs every 2 minutes and checks whether each application
 * needs a new Reddit or Apollo task.
 *
 * RULES:
 * 1. If no task exists, request a new task.
 * 2. If the latest task has pending_count = 0, request a new task.
 * 3. Otherwise, skip creating a duplicate task.
 *
 * REQUIRED SECRETS:
 * SUPABASE_URL
 * SUPABASE_SERVICE_ROLE_KEY
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
// WORKER ENTRY POINT
// ============================================================

export default {
  // ----------------------------------------------------------
  // HTTP HANDLER
  // ----------------------------------------------------------

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (
      url.pathname === "/health" &&
      request.method === "GET"
    ) {
      return Response.json({
        success: true,
        worker: "reportli-planner-cron",
        message: "Worker is running",
        cron: "*/2 * * * *",
        timestamp: new Date().toISOString(),
      });
    }

    if (
      url.pathname === "/run" &&
      request.method === "POST"
    ) {
      try {
        const result = await runCron(env);

        return Response.json(result, {
          status: result.errors.length > 0 ? 207 : 200,
        });
      } catch (error) {
        console.error(
          "Manual cron execution failed:",
          error.message
        );

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
        error: "Route not found",
        available_routes: [
          "GET /health",
          "POST /run",
        ],
      },
      { status: 404 }
    );
  },

  // ----------------------------------------------------------
  // REQUIRED CLOUDFLARE CRON HANDLER
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
// MAIN CRON WORKFLOW
// ============================================================

async function runCron(env) {
  validateEnvironment(env);

  const result = {
    success: true,
    started_at: new Date().toISOString(),
    applications_checked: 0,
    tasks_checked: 0,
    tasks_created: 0,
    tasks_skipped: 0,
    errors: [],
  };

  console.log("========================================");
  console.log("REPORTLI PLANNER CRON STARTED");
  console.log("Time:", result.started_at);
  console.log("========================================");

  // STEP 1: Load applications from Supabase.

  const applications = await getApplications(env);

  result.applications_checked = applications.length;

  console.log(
    "Applications found:",
    applications.length
  );

  // STEP 2: Process each application.

  for (const application of applications) {
    const applicationId = application.id;

    if (!applicationId) {
      result.errors.push({
        error: "Application is missing its ID",
      });

      continue;
    }

    // STEP 3: Process Reddit and Apollo independently.

    for (const source of TASK_SOURCES) {
      result.tasks_checked++;

      try {
        console.log("----------------------------------------");
        console.log("Application:", applicationId);
        console.log("Source:", source);

        const latestTask = await getLatestTask(
          env,
          applicationId,
          source
        );

        // CASE 1: No previous task exists.

        if (latestTask === null) {
          console.log(
            "No existing task. Requesting a new task."
          );

          await createPlannerTask(
            applicationId,
            source
          );

          result.tasks_created++;

          continue;
        }

        // CASE 2: The latest task is completed.

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
            "Latest task is complete. Requesting another task."
          );

          await createPlannerTask(
            applicationId,
            source
          );

          result.tasks_created++;

          continue;
        }

        // CASE 3: Task is pending or its state is unknown.

        console.log(
          "Skipping task creation. pending_count:",
          latestTask.pending_count
        );

        result.tasks_skipped++;

      } catch (error) {
        console.error(
          "Task processing failed:",
          "application =", applicationId,
          "source =", source,
          "error =", error.message
        );

        result.errors.push({
          application_id: String(applicationId),
          source,
          error: error.message,
        });
      }
    }
  }

  result.finished_at = new Date().toISOString();
  result.success = result.errors.length === 0;

  console.log("========================================");
  console.log("REPORTLI PLANNER CRON FINISHED");
  console.log(JSON.stringify(result));
  console.log("========================================");

  return result;
}


// ============================================================
// VALIDATE ENVIRONMENT VARIABLES
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
// SUPABASE REST API HELPER
// ============================================================

async function supabaseRequest(env, path) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const url =
    `${baseUrl}/rest/v1/${path}`;

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
      `Supabase HTTP ${response.status}: ${responseText}`
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
// Expected table: applications
// Expected column: id
// ============================================================

async function getApplications(env) {
  const query = new URLSearchParams({
    select: "id",
    order: "id.asc",
    limit: String(MAX_APPLICATIONS),
  });

  const rows = await supabaseRequest(
    env,
    `applications?${query.toString()}`
  );

  if (!Array.isArray(rows)) {
    throw new Error(
      "Applications response is not an array"
    );
  }

  return rows;
}


// ============================================================
// GET LATEST TASK FOR AN APPLICATION AND SOURCE
//
// Expected table: planner_runs
//
// Expected columns:
// id
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

    application_id:
      `eq.${applicationId}`,

    source:
      `eq.${source}`,

    order:
      "created_at.desc",

    limit:
      "1",
  });

  const rows = await supabaseRequest(
    env,
    `planner_runs?${query.toString()}`
  );

  if (!Array.isArray(rows)) {
    throw new Error(
      "Planner runs response is not an array"
    );
  }

  return rows.length > 0 ? rows[0] : null;
}


// ============================================================
// CREATE TASK THROUGH reportli-ai-planner
//
// POST /create-task
//
// Reddit JSON:
// {
//   "application_id": "app-123",
//   "source": "reddit"
// }
//
// Apollo JSON:
// {
//   "application_id": "app-123",
//   "source": "apollo"
// }
// ============================================================

async function createPlannerTask(
  applicationId,
  source
) {
  if (!TASK_SOURCES.includes(source)) {
    throw new Error(
      `Invalid source: ${source}`
    );
  }

  if (
    applicationId === null ||
    applicationId === undefined ||
    String(applicationId).trim() === ""
  ) {
    throw new Error(
      "Cannot create a task without an application ID"
    );
  }

  // Build the exact JSON expected by the planner Worker.

  const payload = {
    application_id: String(applicationId),
    source: source,
  };

  const requestBody = JSON.stringify(payload);

  console.log("========================================");
  console.log("SENDING POST TO PLANNER WORKER");
  console.log("URL:", PLANNER_CREATE_TASK_URL);
  console.log("Body:", requestBody);
  console.log("========================================");

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
    "Planner HTTP status:",
    response.status
  );

  console.log(
    "Planner response:",
    responseText.slice(0, 2000)
  );

  if (!response.ok) {
    throw new Error(
      `Planner worker returned HTTP ${response.status}: ` +
      responseText
    );
  }

  let data;

  try {
    data = JSON.parse(responseText);
  } catch {
    throw new Error(
      "Planner returned invalid JSON: " +
      responseText.slice(0, 500)
    );
  }

  if (data.success === false) {
    throw new Error(
      data.error ||
      "Planner reported task creation failure"
    );
  }

  console.log(
    "Planner task request succeeded for:",
    applicationId,
    source
  );

  return data;
}


// ============================================================
// FETCH HELPER WITH TIMEOUT
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
