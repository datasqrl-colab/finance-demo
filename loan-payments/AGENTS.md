DataSQRL is a framework for building data pipelines and data products that ingest, process, store, and expose data as data lake/database views or through GraphQL, REST, or MCP APIs. It compiles SQL-like SQRL code into integrated data pipelines using Apache Flink, Postgres, Kafka, Iceberg, Vert.x server and other engines.
DataSQRL runs and tests data pipelines locally or compiles deployment artifacts to deploy by the user or fully-managed by DataSQRL Cloud product offering.

## Development Commands

You are running in **headless mode** inside a DataSQRL container using DataSQRL CLI version 0.11.x to:
- compile a project with: `/opt/agent/cmd.sh compile -r <project root> <configs...> -b <sub-project>` 
- The `build/` folder contains the build artifacts for each sub-project seperately with `-b` flag and the build/<sub-project-name>/deploy/plan folder contains all deployment artifacts for each engine.

You are working on a git repository.

**The compile command is slow and exclusive. Follow these rules without exception:**

1. **Execute one command at a time.** Command invocations are synchronized on a shared lock. A second compile launched while one is running will queue and wait, it will NOT run in parallel. Never launch multiple compile from more than one place simultaneously.
2. **Always run command synchronously in the foreground, and NEVER pipe it into anything.** No `run_in_background`, no `| tail`, `| grep`, `| head`, no `$(...)`. A run starts Postgres, Redpanda and a background helper that all inherit the command's stdout, and they can outlive the command (`head` also kills the build outright via SIGPIPE.) **Redirect to a file and read the file instead:** `/opt/agent/cmd.sh test x-test-package.json > /tmp/test.log 2>&1` then read `/tmp/test.log`.
3. **Command can take multiple minutes.** Flink, Postgres, and Redpanda must start before tests begin. No output for the first minute or two is normal — do not assume failure.
4. **Wait for `COMPILE_DONE: exit_code=N`.** This line marks the end of every compile. Only read results after seeing it. `exit_code=0` means success; any other value means failure.
5. **If you see `COMPILE_QUEUED:`, stop.** Another compile is already running. Do not launch another. Wait for the current one to finish.

## Writing Files

**Write one file per writing call. Never assemble multiple files or an entire project in a single response.**

## Project Structure

```
├── {name}.sqrl                           # Main data processing logic
├── {name}-[dev/test/prod]-package.json   # Configuration per environment
├── {name}-connectors/                    # Project specific source/sink definitions, {name}- is optional
│   └── sources-[dev/test/prod].sqrl      # Source tables by environment
├── snapshots/{name}/                     # Snapshot data for tests, subfolders for multiple projects
├── {name}-api/                           # API definitions (optional)
│   ├── schema.v1.graphqls                # GraphQL schema
│   ├── operations.v1.graphql             # Defines persisted queries, REST endpoints or MCP tools
│   └── tests/                            # Test queries (.graphql)
├── run-tests.sh                          # Runs the test suites — the agent, CI and you all run this one script
└── README.md                             # Project documentation
```

Layouts vary — treat the above as the common shape, not a required one.

Your project may be alone in its repository, or it may sit alongside other SQRL projects and shared
modules. Those are **siblings — outside your project, not part of it**. Their directory names are
whatever the repository author chose, so never assume a particular name: read
`.datasqrl/repo-inventory.md` for what is actually present. For example:

```
{repo root}/                   # mounted READ-ONLY
├── {your-project}/            # the ONLY directory you can write to
├── {another-project}/         # e.g. a second pipeline — reference only
└── {shared-module}/           # e.g. a data catalog or metrics layer — reference only
```

## Configuration

The `*-package.json` files configure every aspect of a DataSQRL project (main script, engines, API, tests, compiler, deployment). 
Invoke the `/configure-sqrl` skill before editing any configuration file (base config and environment overlays, `script.include` for included modules like data catalog). 
Each (sub-)project, and each environment of those (sub-)projects has its own configuration. 
When you create a pipeline project or sub-project, give it a `-test`, a `-dev` and a `-prod` environment overlay. Leave out `-dev` or `-prod` only when the requirements say that the environment is not needed. The `-dev` overlay uses sources and sinks that run on a local machine.
To prepare tests, compile and run tests; first invoke the `/test-sqrl` skill and then apply the steps.
Always check the `package.json` files for authoritative file paths, and align `README.md` with the project-specific settings.

## Skill Usage Policy

Before any file change, work through this checklist:

1. State what you are about to do (file type, what it changes).
2. Check each skill's trigger condition against your action and invoke every match:

| Trigger condition                                                                                                     | Skill                      | Required?                                          |
|-----------------------------------------------------------------------------------------------------------------------|----------------------------|----------------------------------------------------|
                                                                                                          | Writing or editing a `.sqrl` file                                                                                     | `/implement-sqrl`          | If applicable                                      |
| Writing any source/sink definition file (typically a `connectors/` folder, or a catalog you own like `data_catalog/`) | `/manage-connector`        | **MUST** — even when following an existing pattern |
| Editing a `*-package.json`                                                                                            | `/configure-sqrl`          | If applicable                                      |
| Adding or modifying a SUBSCRIBE, mutation table, or `.graphql`/`.graphqls` API schema                                 | `/design-api`              | **MUST** — including on first creation             |
| When requirements specify the API schema that the SQRL project requires                                               | `/from-api`                | **MUST** — including on first creation             |
| Writing a `/*+test*/` table, modifying snapshots, creating or editing `run-tests.sh`, or first test run on a project  | `/test-sqrl`               | **MUST** — no exceptions                           |
| Creating or editing data catalog                                                                                      | `/build-catalog`           | **MUST** - no excpetions                           |
| Test or compile error not immediately obvious                                                                         | `/troubleshoot-test`       | If applicable                                      |
| Given provided source/sample/test data files to inspect before modeling                                               | `/data-observation`        | If applicable                                      |
| Writing or updating `README.md` or any other documentation                                                            | `/write-documentation`     | If applicable                                      |
| A large data file is present, or a run reports `LARGE_DATA_BLOCKED`                                                   | `/handle-large-data-files` | If applicable — reactive                     |

3. For non-MUST skills: only skip if you explicitly state why it does not apply to this specific action.
4. For **MUST** skills: "I already know the pattern" is not a valid reason to skip.
5. **Skill form takes precedence over plan-step wording.** Where a plan step and a skill differ on import form, connector pattern, API exposure, hint placement, or syntax, implement the skill's form, then correct the step's text to describe what you built. Plan steps state intent and are written without a compiler.

Invoke skills immediately before the file change they apply to — not all at once at session start.

Consult skills before reading templates — skills contain the essential patterns; templates are for structural reference only.


## Other Projects in the Repository — Read-Only

Your project can sit inside a repository that also holds other SQRL projects and shared modules
(a data catalog is the common case, but there may be several, with any names). The whole repository
is mounted **READ-ONLY**; only your own project directory is writable.
`.datasqrl/repo-inventory.md` lists what is actually there — rely on it rather than on assumed names
or layouts.

- **Never** create, edit, copy or delete anything outside your project.
- **Never copy** a sibling's files into your project. If you need what a shared module provides,
  declare it under `script.include` and import it by namespace.
- **Never read another project's build folders** (`build/`, `build-*/`) — they are transient compiler output. The `.sqrl`, `package.json`, and `README.md` files in the project folder contain all relevant information
- Only include modules that this project **actually needs**. 
- Sibling projects are useful as **worked examples**: how they configure `script.include`, how they
  import a module, how they lay out connectors and tests. Follow those patterns where they fit.

## Scope — Do Not Change What You Were Not Asked to Change

A patch run changes only what the request asks for, plus whatever that change breaks.

- Touch a file only if the request names it, or it must change for the request to compile or pass its tests.
- Update the project documentation according to changes.
- **"For consistency with the new code" is never a reason to rewrite existing code.** Match the
  existing code's conventions in what you add; never migrate the existing code to match yours unless explicitly asked.

## Data Processing

The data ingestion, processing, and serving logic is implemented in the main script using a SQL dialect called SQRL which is an extension of Flink SQL 2.3. The main script may import additional SQRL scripts: local ones by relative path, scripts of another SQRL project (e.g. a shared data catalog) by the namespace declared under configuration file: invoke the `/configure-sqrl` skill (Included Projects) and the `/implement-sqrl` skill (SQRL Language Spec → IMPORT Statement). SQRL scripts can import Flink functions implemented in Java for custom processing logic that cannot be expressed in SQL: invoke the `/implement-udf` skill.

All data processing is implemented in SQRL scripts and imported functions.

## Sources and Sinks

Source/sink tables are defined with `CREATE TABLE` statements - often in dedicated SQRL files separated by environment in a connectors folder and imported/exported from the main script.

Sources are used to ingest data for processing. Sinks are used to export data to external systems. `CREATE TABLE` statements with a preceding `/*+engine(..) */` hint are sources/sinks that are managed by DataSQRL with the external resources (e.g. Kafka topic or Iceberg table) created and managed automatically.

Sources and sinks local to the project (usually in a `connectors` folder) can be updated when the request requires it — see **Scope** above. Sources, sinks and imported view definitions that come from a shared module (a data catalog or similar, whatever it is named) belong to other teams/projects and cannot be changed — they are mounted read-only. Always check whether an import is local or comes from a shared module before making changes.

## API

DataSQRL uses GraphQL as the authoritative model for the data serving API which exposes the processed data if the server engine is enabled. The GraphQL API maps tables in the main script (and those imported inline) to queries/mutations/subscriptions plus associated types and relationships to fields. SQRL supports table functions to define custom endpoints.
The GraphQL schema is either inferred from SQRL or defined explicitly and configured in the package.json. The inferred GraphQL schema is written to `<build-folder>/inferred_schema.graphqls` after compilation (the folder named by `-b`, else `build/`).

REST and MCP (model context protocol) endpoints are created automatically for each GraphQL query and mutation or defined explicitly as GraphQL operations.

## Testing

The test command executes the GraphQL queries in the configured test folder and the table queries marked with the `/*+test*/` hint that the test server's schema exposes, and stores the results in the snapshots folder. Tests pass when snapshots are identical or no records are returned on a test marked `no_rows`. `${DATA_PATH}` is set by the framework and does not need to be configured. Invoke the `/test-sqrl` skill for writing tests and test data, and for running the test packages.

## Documentation

Clearly and concisely document any code you implement focusing on implementation choices and intent. Do not document what is implemented but focus on the *why?*.