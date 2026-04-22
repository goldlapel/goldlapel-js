// Shared integration-test gating — standardized across all Gold Lapel wrappers.
//
// Convention:
//   - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate ("yes, really run these")
//   - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream
//
// Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM
// is missing, we fail loudly — prevents a half-configured CI from silently
// skipping integration tests and producing a false-green unit-only run.
//
// If GOLDLAPEL_INTEGRATION is unset, integration tests skip silently.

// Evaluates the gate and returns { shouldRun, upstream, skipReason, failReason }.
// Tests wire this into node:test's `skip` option, or throw on failReason.
export function integrationGate() {
    const integration = process.env.GOLDLAPEL_INTEGRATION === '1';
    const upstream = process.env.GOLDLAPEL_TEST_UPSTREAM;

    if (integration && !upstream) {
        return {
            shouldRun: false,
            upstream: null,
            skipReason: null,
            failReason:
                'GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is ' +
                'missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL ' +
                '(e.g. postgresql://postgres@localhost/postgres) or unset ' +
                'GOLDLAPEL_INTEGRATION to skip integration tests.',
        };
    }

    if (!integration) {
        return {
            shouldRun: false,
            upstream: null,
            skipReason:
                'set GOLDLAPEL_INTEGRATION=1 and GOLDLAPEL_TEST_UPSTREAM to run',
            failReason: null,
        };
    }

    return { shouldRun: true, upstream, skipReason: null, failReason: null };
}
