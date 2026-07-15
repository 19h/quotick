//! Immutable versioned UTC trading schedules and order-lifetime resolution.

use std::fmt;
use std::sync::Arc;

use crate::domain::{
    AccountingDate, CalendarId, CalendarVersion, CommandId, InstrumentId, InstrumentVersion,
    TimestampNs, TradingSessionId,
};
use crate::matching::{ExpirySweep, TimeInForce};

/// A contradiction in a trading session or canonical calendar image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradingCalendarError {
    /// The order-entry interval was empty or reversed.
    InvalidEntryWindow {
        /// Session containing the invalid interval.
        session_id: TradingSessionId,
    },
    /// The session-order deadline preceded the end of order entry.
    SessionExpiryBeforeEntryClose {
        /// Session containing the invalid deadline.
        session_id: TradingSessionId,
    },
    /// The trading-day deadline preceded the session-order deadline.
    DayExpiryBeforeSessionExpiry {
        /// Session containing the invalid deadline.
        session_id: TradingSessionId,
    },
    /// A calendar contained no sessions.
    EmptySchedule,
    /// The generation became effective after its first entry window opened.
    EffectiveAfterFirstEntry {
        /// Generation effective time.
        effective_from: TimestampNs,
        /// First entry-window opening time.
        first_entry: TimestampNs,
    },
    /// A session-order lifetime overlapped the next session's entry window.
    SessionOverlap {
        /// Earlier session.
        previous: TradingSessionId,
        /// Later session.
        next: TradingSessionId,
    },
    /// Sessions assigned to one trading date disagreed on its expiry boundary.
    InconsistentDayExpiry {
        /// Trading date with conflicting boundaries.
        trading_date: AccountingDate,
    },
    /// Trading dates were not nondecreasing in entry-window order.
    TradingDateRegression {
        /// Earlier session's trading date.
        previous: AccountingDate,
        /// Later session's trading date.
        next: AccountingDate,
    },
    /// A prior trading day's deadline overlapped the next trading date.
    TradingDayOverlap {
        /// Earlier trading date.
        previous: AccountingDate,
        /// Later trading date.
        next: AccountingDate,
    },
    /// A session identifier occurred more than once.
    DuplicateSessionId {
        /// Duplicate identifier.
        session_id: TradingSessionId,
    },
    /// The immutable lookup index could not reserve its exact finite size.
    IndexReservationFailed {
        /// Number of sessions requiring index entries.
        sessions: usize,
    },
    /// A calendar-relative lifetime was requested outside order-entry hours.
    NoActiveSession {
        /// Resolution time.
        at: TimestampNs,
    },
    /// A requested session identifier was not present in this generation.
    UnknownSession {
        /// Missing identifier.
        session_id: TradingSessionId,
    },
    /// An expiry control was constructed before its inclusive boundary.
    ControlBeforeBoundary {
        /// Calendar-selected expiry boundary.
        boundary: TimestampNs,
        /// Control receive time.
        received_at: TimestampNs,
    },
}

impl fmt::Display for TradingCalendarError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEntryWindow { session_id } => {
                write!(
                    formatter,
                    "trading session {session_id} has an invalid entry window"
                )
            }
            Self::SessionExpiryBeforeEntryClose { session_id } => write!(
                formatter,
                "trading session {session_id} expires session orders before entry closes"
            ),
            Self::DayExpiryBeforeSessionExpiry { session_id } => write!(
                formatter,
                "trading session {session_id} expires day orders before session orders"
            ),
            Self::EmptySchedule => formatter.write_str("trading calendar has no sessions"),
            Self::EffectiveAfterFirstEntry {
                effective_from,
                first_entry,
            } => write!(
                formatter,
                "calendar effective time {} follows first entry time {}",
                effective_from.as_unix_nanos(),
                first_entry.as_unix_nanos()
            ),
            Self::SessionOverlap { previous, next } => write!(
                formatter,
                "trading session {previous} overlaps later session {next}"
            ),
            Self::InconsistentDayExpiry { trading_date } => write!(
                formatter,
                "trading date {} has inconsistent day-order expiry boundaries",
                trading_date.days_since_unix_epoch()
            ),
            Self::TradingDateRegression { previous, next } => write!(
                formatter,
                "trading date {} regresses to {}",
                previous.days_since_unix_epoch(),
                next.days_since_unix_epoch()
            ),
            Self::TradingDayOverlap { previous, next } => write!(
                formatter,
                "trading date {} overlaps later trading date {}",
                previous.days_since_unix_epoch(),
                next.days_since_unix_epoch()
            ),
            Self::DuplicateSessionId { session_id } => {
                write!(
                    formatter,
                    "trading session identifier {session_id} is duplicated"
                )
            }
            Self::IndexReservationFailed { sessions } => write!(
                formatter,
                "could not reserve trading-calendar index for {sessions} sessions"
            ),
            Self::NoActiveSession { at } => write!(
                formatter,
                "no order-entry session is active at {}",
                at.as_unix_nanos()
            ),
            Self::UnknownSession { session_id } => {
                write!(
                    formatter,
                    "trading session {session_id} is not in this calendar"
                )
            }
            Self::ControlBeforeBoundary {
                boundary,
                received_at,
            } => write!(
                formatter,
                "expiry boundary {} follows control receive time {}",
                boundary.as_unix_nanos(),
                received_at.as_unix_nanos()
            ),
        }
    }
}

impl std::error::Error for TradingCalendarError {}

/// One validated UTC order-entry session and its lifetime boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradingSession {
    session_id: TradingSessionId,
    trading_date: AccountingDate,
    order_entry_opens_at: TimestampNs,
    order_entry_closes_at: TimestampNs,
    session_orders_expire_at: TimestampNs,
    day_orders_expire_at: TimestampNs,
}

impl TradingSession {
    /// Validates one session row.
    ///
    /// # Errors
    ///
    /// Returns [`TradingCalendarError`] when the entry window is empty or a
    /// lifetime boundary precedes the boundary it depends on.
    pub const fn new(
        session_id: TradingSessionId,
        trading_date: AccountingDate,
        order_entry_opens_at: TimestampNs,
        order_entry_closes_at: TimestampNs,
        session_orders_expire_at: TimestampNs,
        day_orders_expire_at: TimestampNs,
    ) -> Result<Self, TradingCalendarError> {
        if order_entry_opens_at.as_unix_nanos() >= order_entry_closes_at.as_unix_nanos() {
            return Err(TradingCalendarError::InvalidEntryWindow { session_id });
        }
        if session_orders_expire_at.as_unix_nanos() < order_entry_closes_at.as_unix_nanos() {
            return Err(TradingCalendarError::SessionExpiryBeforeEntryClose { session_id });
        }
        if day_orders_expire_at.as_unix_nanos() < session_orders_expire_at.as_unix_nanos() {
            return Err(TradingCalendarError::DayExpiryBeforeSessionExpiry { session_id });
        }
        Ok(Self {
            session_id,
            trading_date,
            order_entry_opens_at,
            order_entry_closes_at,
            session_orders_expire_at,
            day_orders_expire_at,
        })
    }

    /// Returns the stable session identity.
    #[must_use]
    pub const fn session_id(self) -> TradingSessionId {
        self.session_id
    }

    /// Returns the accounting date assigned by the calendar publisher.
    #[must_use]
    pub const fn trading_date(self) -> AccountingDate {
        self.trading_date
    }

    /// Returns the inclusive order-entry opening time.
    #[must_use]
    pub const fn order_entry_opens_at(self) -> TimestampNs {
        self.order_entry_opens_at
    }

    /// Returns the exclusive order-entry closing time.
    #[must_use]
    pub const fn order_entry_closes_at(self) -> TimestampNs {
        self.order_entry_closes_at
    }

    /// Returns the inclusive good-for-session expiry boundary.
    #[must_use]
    pub const fn session_orders_expire_at(self) -> TimestampNs {
        self.session_orders_expire_at
    }

    /// Returns the inclusive day-order expiry boundary.
    #[must_use]
    pub const fn day_orders_expire_at(self) -> TimestampNs {
        self.day_orders_expire_at
    }

    const fn entry_contains(self, at: TimestampNs) -> bool {
        self.order_entry_opens_at.as_unix_nanos() <= at.as_unix_nanos()
            && at.as_unix_nanos() < self.order_entry_closes_at.as_unix_nanos()
    }
}

/// Calendar-relative order lifetime accepted at an ingress boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CalendarTimeInForce {
    /// An already absolute or immediate matching-engine lifetime.
    Native(TimeInForce),
    /// Rest through the active session's session-order expiry boundary.
    GoodForSession,
    /// Rest through the active session's trading-day expiry boundary.
    Day,
}

/// A calendar-relative lifetime resolved to matching-engine semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedTimeInForce {
    calendar_id: CalendarId,
    calendar_version: CalendarVersion,
    normalized: TimeInForce,
    session_id: Option<TradingSessionId>,
    trading_date: Option<AccountingDate>,
}

impl ResolvedTimeInForce {
    /// Returns the calendar identity used for resolution.
    #[must_use]
    pub const fn calendar_id(self) -> CalendarId {
        self.calendar_id
    }

    /// Returns the immutable calendar version used for resolution.
    #[must_use]
    pub const fn calendar_version(self) -> CalendarVersion {
        self.calendar_version
    }

    /// Returns the matching-engine lifetime.
    #[must_use]
    pub const fn normalized(self) -> TimeInForce {
        self.normalized
    }

    /// Returns the active session when resolution occurred during entry hours.
    #[must_use]
    pub const fn session_id(self) -> Option<TradingSessionId> {
        self.session_id
    }

    /// Returns the active trading date when resolution occurred during entry hours.
    #[must_use]
    pub const fn trading_date(self) -> Option<AccountingDate> {
        self.trading_date
    }
}

/// Calendar boundary selected for an explicit matching-engine expiry sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderExpiryBoundary {
    /// The session-order expiry boundary.
    Session,
    /// The trading-day expiry boundary.
    TradingDay,
}

/// An immutable canonical trading-calendar generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradingCalendar {
    calendar_id: CalendarId,
    version: CalendarVersion,
    effective_from: TimestampNs,
    sessions: Arc<Vec<TradingSession>>,
    sessions_by_id: Arc<Vec<(TradingSessionId, usize)>>,
}

impl TradingCalendar {
    /// Validates and indexes one immutable generation.
    ///
    /// Sessions must already be in strictly non-overlapping entry-time order.
    /// Trading dates must be nondecreasing, and all sessions on one date must
    /// carry one day-order boundary.
    ///
    /// # Errors
    ///
    /// Returns [`TradingCalendarError`] for an empty or noncanonical schedule,
    /// a duplicate identifier, or index reservation failure.
    pub fn try_new(
        calendar_id: CalendarId,
        version: CalendarVersion,
        effective_from: TimestampNs,
        sessions: Vec<TradingSession>,
    ) -> Result<Self, TradingCalendarError> {
        let Some(first) = sessions.first().copied() else {
            return Err(TradingCalendarError::EmptySchedule);
        };
        if effective_from > first.order_entry_opens_at {
            return Err(TradingCalendarError::EffectiveAfterFirstEntry {
                effective_from,
                first_entry: first.order_entry_opens_at,
            });
        }

        for pair in sessions.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            if previous.session_orders_expire_at > next.order_entry_opens_at {
                return Err(TradingCalendarError::SessionOverlap {
                    previous: previous.session_id,
                    next: next.session_id,
                });
            }
            if previous.trading_date > next.trading_date {
                return Err(TradingCalendarError::TradingDateRegression {
                    previous: previous.trading_date,
                    next: next.trading_date,
                });
            }
            if previous.trading_date == next.trading_date {
                if previous.day_orders_expire_at != next.day_orders_expire_at {
                    return Err(TradingCalendarError::InconsistentDayExpiry {
                        trading_date: previous.trading_date,
                    });
                }
            } else if previous.day_orders_expire_at > next.order_entry_opens_at {
                return Err(TradingCalendarError::TradingDayOverlap {
                    previous: previous.trading_date,
                    next: next.trading_date,
                });
            }
        }

        let mut sessions_by_id = Vec::new();
        sessions_by_id
            .try_reserve_exact(sessions.len())
            .map_err(|_| TradingCalendarError::IndexReservationFailed {
                sessions: sessions.len(),
            })?;
        sessions_by_id.extend(
            sessions
                .iter()
                .enumerate()
                .map(|(index, session)| (session.session_id, index)),
        );
        sessions_by_id.sort_unstable_by_key(|(session_id, _)| *session_id);
        if let Some(pair) = sessions_by_id
            .windows(2)
            .find(|pair| pair[0].0 == pair[1].0)
        {
            return Err(TradingCalendarError::DuplicateSessionId {
                session_id: pair[0].0,
            });
        }

        Ok(Self {
            calendar_id,
            version,
            effective_from,
            sessions: Arc::new(sessions),
            sessions_by_id: Arc::new(sessions_by_id),
        })
    }

    /// Returns the stable calendar identity.
    #[must_use]
    pub const fn calendar_id(&self) -> CalendarId {
        self.calendar_id
    }

    /// Returns the immutable calendar version.
    #[must_use]
    pub const fn version(&self) -> CalendarVersion {
        self.version
    }

    /// Returns the generation effective time.
    #[must_use]
    pub const fn effective_from(&self) -> TimestampNs {
        self.effective_from
    }

    /// Returns sessions in canonical entry-time order.
    #[must_use]
    pub fn sessions(&self) -> &[TradingSession] {
        self.sessions.as_slice()
    }

    /// Returns the session whose half-open entry window contains `at`.
    #[must_use]
    pub fn active_session(&self, at: TimestampNs) -> Option<&TradingSession> {
        let insertion = self
            .sessions
            .partition_point(|session| session.order_entry_opens_at <= at);
        insertion
            .checked_sub(1)
            .and_then(|index| self.sessions.get(index))
            .filter(|session| session.entry_contains(at))
    }

    /// Returns the first session whose entry window opens strictly after `at`.
    #[must_use]
    pub fn next_session_after(&self, at: TimestampNs) -> Option<&TradingSession> {
        let index = self
            .sessions
            .partition_point(|session| session.order_entry_opens_at <= at);
        self.sessions.get(index)
    }

    /// Returns a session by stable identifier.
    #[must_use]
    pub fn session(&self, session_id: TradingSessionId) -> Option<&TradingSession> {
        self.sessions_by_id
            .binary_search_by_key(&session_id, |(candidate, _)| *candidate)
            .ok()
            .and_then(|index| self.sessions_by_id.get(index))
            .and_then(|(_, session_index)| self.sessions.get(*session_index))
    }

    /// Returns the contiguous sessions assigned to `trading_date`.
    #[must_use]
    pub fn sessions_on(&self, trading_date: AccountingDate) -> &[TradingSession] {
        let start = self
            .sessions
            .partition_point(|session| session.trading_date < trading_date);
        let end = self
            .sessions
            .partition_point(|session| session.trading_date <= trading_date);
        &self.sessions[start..end]
    }

    /// Resolves a gateway lifetime against the active entry session.
    ///
    /// Native lifetimes are returned unchanged and do not require an active
    /// session. Calendar-relative lifetimes require one.
    ///
    /// # Errors
    ///
    /// Returns [`TradingCalendarError::NoActiveSession`] when a day or session
    /// lifetime is requested outside all entry windows.
    pub fn resolve_time_in_force(
        &self,
        requested: CalendarTimeInForce,
        received_at: TimestampNs,
    ) -> Result<ResolvedTimeInForce, TradingCalendarError> {
        let active = self.active_session(received_at).copied();
        let normalized = match requested {
            CalendarTimeInForce::Native(time_in_force) => time_in_force,
            CalendarTimeInForce::GoodForSession => {
                let session =
                    active.ok_or(TradingCalendarError::NoActiveSession { at: received_at })?;
                TimeInForce::GoodTilTimestamp {
                    expires_at: session.session_orders_expire_at,
                }
            }
            CalendarTimeInForce::Day => {
                let session =
                    active.ok_or(TradingCalendarError::NoActiveSession { at: received_at })?;
                TimeInForce::GoodTilTimestamp {
                    expires_at: session.day_orders_expire_at,
                }
            }
        };
        Ok(ResolvedTimeInForce {
            calendar_id: self.calendar_id,
            calendar_version: self.version,
            normalized,
            session_id: active.map(TradingSession::session_id),
            trading_date: active.map(TradingSession::trading_date),
        })
    }

    /// Constructs an identity-bound expiry command at a calendar boundary.
    ///
    /// # Errors
    ///
    /// Returns [`TradingCalendarError::UnknownSession`] for an absent identity,
    /// or [`TradingCalendarError::ControlBeforeBoundary`] when `received_at`
    /// precedes the selected inclusive boundary.
    pub fn expiry_sweep(
        &self,
        session_id: TradingSessionId,
        boundary: OrderExpiryBoundary,
        command_id: CommandId,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        received_at: TimestampNs,
    ) -> Result<ExpirySweep, TradingCalendarError> {
        let session = self
            .session(session_id)
            .copied()
            .ok_or(TradingCalendarError::UnknownSession { session_id })?;
        let through = match boundary {
            OrderExpiryBoundary::Session => session.session_orders_expire_at,
            OrderExpiryBoundary::TradingDay => session.day_orders_expire_at,
        };
        if received_at < through {
            return Err(TradingCalendarError::ControlBeforeBoundary {
                boundary: through,
                received_at,
            });
        }
        Ok(ExpirySweep {
            command_id,
            instrument_id,
            instrument_version,
            through,
            received_at,
        })
    }

    /// Returns whether two values share both immutable schedule allocations.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.sessions, &other.sessions)
            && Arc::ptr_eq(&self.sessions_by_id, &other.sessions_by_id)
    }
}
