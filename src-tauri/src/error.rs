use serde::ser::SerializeMap;

#[derive(Debug)]
pub enum AppError {
    FileError(String),
    QueryError(String),
    AuthError(String),
    ExportError(String),
    ConnectorError(String),
    GraphError(String),
    EtlError(String),
}

impl AppError {
    fn message(&self) -> &str {
        match self {
            AppError::FileError(msg) => msg,
            AppError::QueryError(msg) => msg,
            AppError::AuthError(msg) => msg,
            AppError::ExportError(msg) => msg,
            AppError::ConnectorError(msg) => msg,
            AppError::GraphError(msg) => msg,
            AppError::EtlError(msg) => msg,
        }
    }

    fn code(&self) -> &str {
        match self {
            AppError::FileError(_) => "FILE_ERROR",
            AppError::QueryError(_) => "QUERY_ERROR",
            AppError::AuthError(_) => "AUTH_ERROR",
            AppError::ExportError(_) => "EXPORT_ERROR",
            AppError::ConnectorError(_) => "CONNECTOR_ERROR",
            AppError::GraphError(_) => "GRAPH_ERROR",
            AppError::EtlError(_) => "ETL_ERROR",
        }
    }
}

impl serde::Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("error", &true)?;
        map.serialize_entry("message", self.message())?;
        map.serialize_entry("code", self.code())?;
        map.end()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl From<duckdb::Error> for AppError {
    fn from(err: duckdb::Error) -> Self {
        AppError::QueryError(err.to_string())
    }
}

impl From<duckdb::arrow::error::ArrowError> for AppError {
    fn from(err: duckdb::arrow::error::ArrowError) -> Self {
        AppError::QueryError(format!("Arrow error: {}", err))
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::FileError(err.to_string())
    }
}
