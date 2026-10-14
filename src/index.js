/**

* Reportli Planner Cron Worker
* Version: 1.1.0
* 
* Runs every 2 minutes.
* 
* Rules:
* 1. Create a Reddit task if none exists.
* 2. Create an Apollo task if none exists.
* 3. Create a new task if the latest task has pending_count = 0.
* 4. Skip sources whose latest task has pending_count > 0.
* 
* Required secrets:
* SUPABASE_URL
* SUPABASE_SERVICE_ROLE_KEY
  */

const VERSION = "1.1.0";

const PLANNER_CREATE_TASK_URL =
"https://reportli-ai-planner.reportliaihq.workers.dev/create-task";

const SOURCES = ["reddit", "apollo"];
const MAX_CONCURRENT_REQUESTS = 3;
const REQUEST_TIMEOUT_MS = 20000;

export default {
// Runs automatically when the Cloudflare Cron Trigger fires.
async scheduled(controller, env, ctx) {
ctx.waitUntil(
runCron(env).catch((error) => {
console.error("Scheduled cron failed:", error);
})
);
},

async fetch(request, env) {
const url = new URL(request.url);

if (request.method === "OPTIONS") {
  return new Response(null, {
    status: 204,
    headers: corsHeaders(),
  });
}

if (url.pathname === "/health" && request.method === "GET") {
  return jsonResponse({
    success: true,
    worker: "reportli-planner-cron",
    version: VERSION,
    message: "Worker is running.",
  });
}

if (url.pathname === "/run" && request.method === "POST") {
  try {
    const result = await runCron(env);

    return jsonResponse({
      success: result.errors.length === 0,
      worker: "reportli-planner-cron",
      version: VERSION,
      result,
    }, result.errors.length === 0 ? 200 : 207);
  } catch (error) {
    console.error("Manual cron run failed:", error);

    return jsonResponse({
      success: false,
      error: error.message || "Cron run failed.",
    }, 500);
  }
}

return jsonResponse({
  success: false,
  error: "Route not found.",
  available_routes: [
    "GET /health",
    "POST /run",
  ],
}, 404);

},
};

// ------------------------------------------------------
// MAIN CRON LOGIC
// ------------------------------------------------------

async function runCron(env) {
validateEnvironment(env);

const startedAt = new Date().toISOString();

console.log("[${VERSION}] Cron started at ${startedAt}");

const applications = await getApplications(env);

console.log("Found ${applications.length} applications.");

const summary = {
started_at: startedAt,
applications_found: applications.length,
tasks_checked: 0,
tasks_created: 0,
tasks_skipped: 0,
errors: [],
results: [],
};

const latestTasks = await getLatestTasksForApplications(
env,
applications
);

const tasksToCreate = [];

for (const application of applications) {
if (!application.id) {
continue;
}

for (const source of SOURCES) {
  summary.tasks_checked++;

  const latestTask = latestTasks.get(
    makeTaskKey(application.id, source)
  );

  if (!latestTask) {
    tasksToCreate.push({
      application_id: application.id,
      source,
      reason: "missing_task",
    });
    continue;
  }

  const pendingCount = Number(latestTask.pending_count);

  if (pendingCount === 0) {
    tasksToCreate.push({
      application_id: application.id,
      source,
      reason: "pending_count_zero",
      previous_task_id: latestTask.id || null,
    });
    continue;
  }

  summary.tasks_skipped++;

  console.log(
    `Skipping ${source} for application ${application.id}; ` +
    `latest pending_count=${latestTask.pending_count}`
  );
}

}

console.log(
"Tasks requiring creation: ${tasksToCreate.length}"
);

await runWithConcurrency(
tasksToCreate,
MAX_CONCURRENT_REQUESTS,
async (task) => {
try {
const response = await createPlannerTask(task);

    summary.tasks_created++;

    summary.results.push({
      application_id: task.application_id,
      source: task.source,
      reason: task.reason,
      success: true,
      response,
    });
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
      `${task.application_id}: ${entry.error}`
    );
  }
}

);

summary.finished_at = new Date().toISOString();

console.log("Cron finished:", JSON.stringify({
applications_found: summary.applications_found,
tasks_checked: summary.tasks_checked,
tasks_created: summary.tasks_created,
tasks_skipped: summary.tasks_skipped,
errors: summary.errors.length,
}));

return summary;
}

// ------------------------------------------------------
// READ APPLICATIONS FROM SUPABASE
// ------------------------------------------------------

async function getApplications(env) {
const url = new URL(
"${env.SUPABASE_URL}/rest/v1/applications"
);

url.searchParams.set("select", "id,user_id");
url.searchParams.set("order", "id.asc");
url.searchParams.set("limit", "1000");

const response = await supabaseFetch(env, url.toString());

if (!response.ok) {
throw new Error(
"Failed to fetch applications: HTTP ${response.status}: " +
await response.text()
);
}

const data = await response.json();

if (!Array.isArray(data)) {
throw new Error("Supabase returned an invalid applications response.");
}

return data;
}

// ------------------------------------------------------
// READ THE LATEST TASK FOR EACH APPLICATION AND SOURCE
// ------------------------------------------------------

async function getLatestTasksForApplications(env, applications) {
const latestTasks = new Map();
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
"${env.SUPABASE_URL}/rest/v1/planner_runs"
);

  url.searchParams.set(
    "select",
    "id,application_id,source,pending_count,created_at"
  );
  url.searchParams.set(
    "application_id",
    `eq.${lookup.application_id}`
  );
  url.searchParams.set("source", `eq.${lookup.source}`);
  url.searchParams.set("order", "created_at.desc");
  url.searchParams.set("limit", "1");

  const response = await supabaseFetch(env, url.toString());

  if (!response.ok) {
    throw new Error(
      `Failed to read planner_runs for application ` +
      `${lookup.application_id}, source ${lookup.source}: ` +
      `HTTP ${response.status}: ${await response.text()}`
    );
  }

  const rows = await response.json();

  if (Array.isArray(rows) && rows.length > 0) {
    latestTasks.set(
      makeTaskKey(lookup.application_id, lookup.source),
      rows[0]
    );
  }
}

);

return latestTasks;
}

// ------------------------------------------------------
// CALL THE EXISTING PLANNER WORKER
// ------------------------------------------------------

async function createPlannerTask(task) {
const payload = {
application_id: String(task.application_id),
source: task.source,
};

if (!["reddit", "apollo"].includes(payload.source)) {
throw new Error("Invalid source: ${payload.source}");
}

// Serialize the object once, producing valid JSON.
const requestBody = JSON.stringify(payload);

console.log(
"Sending task request: ${requestBody}; reason=${task.reason}"
);

const controller = new AbortController();

const timeout = setTimeout(
() => controller.abort(),
REQUEST_TIMEOUT_MS
);

let response;

try {
response = await fetch(PLANNER_CREATE_TASK_URL, {
method: "POST",
headers: {
"Content-Type": "application/json",
"Accept": "application/json",
},
body: requestBody,
signal: controller.signal,
});
} catch (error) {
if (error.name === "AbortError") {
throw new Error(
"Planner request timed out after ${REQUEST_TIMEOUT_MS} ms."
);
}

throw new Error(`Could not reach planner worker: ${error.message}`);

} finally {
clearTimeout(timeout);
}

const responseText = await response.text();

let responseData;

try {
responseData = responseText ? JSON.parse(responseText) : {};
} catch {
responseData = { raw_response: responseText };
}

console.log(
"Planner response for ${task.source}, application " +
"${task.application_id}: HTTP ${response.status}; " +
"${JSON.stringify(responseData)}"
);

if (!response.ok) {
throw new Error(
"Planner returned HTTP ${response.status}: " +
JSON.stringify(responseData)
);
}

if (
responseData &&
typeof responseData === "object" &&
responseData.success === false
) {
throw new Error(
"Planner reported failure: ${JSON.stringify(responseData)}"
);
}

return responseData;
}

// ------------------------------------------------------
// SUPABASE REST HELPER
// ------------------------------------------------------

async function supabaseFetch(env, url) {
return fetch(url, {
method: "GET",
headers: {
"apikey": env.SUPABASE_SERVICE_ROLE_KEY,
"Authorization": "Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}",
"Accept": "application/json",
},
});
}

// ------------------------------------------------------
// CONCURRENCY HELPER
// ------------------------------------------------------

async function runWithConcurrency(items, concurrency, callback) {
const results = new Array(items.length);
let nextIndex = 0;

const workerCount = Math.min(concurrency, items.length);

const workers = Array.from({ length: workerCount }, async () => {
while (true) {
const index = nextIndex++;

  if (index >= items.length) {
    return;
  }

  results[index] = await callback(items[index]);
}

});

await Promise.all(workers);

return results;
}

// ------------------------------------------------------
// UTILITIES
// ------------------------------------------------------

function makeTaskKey(applicationId, source) {
return "${String(applicationId)}::${String(source)}";
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
"Missing Cloudflare secrets: ${missing.join(", ")}"
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
