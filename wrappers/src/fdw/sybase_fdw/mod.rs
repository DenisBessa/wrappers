#![allow(clippy::module_inception)]
mod sybase_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum SybaseFdwError {
    #[error("ODBC error: {0}")]
    OdbcError(String),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),
}

impl From<odbc_api::Error> for SybaseFdwError {
    fn from(err: odbc_api::Error) -> Self {
        SybaseFdwError::OdbcError(format!("{}", err))
    }
}

impl From<SybaseFdwError> for ErrorReport {
    fn from(value: SybaseFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type SybaseFdwResult<T> = Result<T, SybaseFdwError>;
