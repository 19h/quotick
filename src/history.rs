//! Zero-copy views over bounded command/report history.

/// One borrowed command and its canonical non-replayed execution report.
///
/// Matching runtimes retain these values for exact idempotency. This view
/// exposes that same storage without cloning a report, copying its event trace,
/// allocating an output collection, or constructing a checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedCommandReport<'a, C, R> {
    command: &'a C,
    report: &'a R,
}

impl<'a, C, R> RetainedCommandReport<'a, C, R> {
    pub(crate) const fn new(command: &'a C, report: &'a R) -> Self {
        Self { command, report }
    }

    /// Returns the exact command retained for idempotency.
    #[must_use]
    pub const fn command(&self) -> &'a C {
        self.command
    }

    /// Returns the original canonical report with `replayed = false`.
    #[must_use]
    pub const fn report(&self) -> &'a R {
        self.report
    }
}
