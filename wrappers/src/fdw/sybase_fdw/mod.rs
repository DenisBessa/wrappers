#![allow(clippy::module_inception)]
mod join;
mod sybase_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
pub(crate) enum SybaseFdwError {
    #[error("ODBC error: {0}")]
    Odbc(String),

    #[error("{0}")]
    CreateRuntime(#[from] CreateRuntimeError),

    #[error("{0}")]
    Options(#[from] OptionsError),
}

impl From<odbc_api::Error> for SybaseFdwError {
    fn from(err: odbc_api::Error) -> Self {
        SybaseFdwError::Odbc(format!("{err}"))
    }
}

impl From<SybaseFdwError> for ErrorReport {
    fn from(value: SybaseFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

pub(crate) type SybaseFdwResult<T> = Result<T, SybaseFdwError>;
