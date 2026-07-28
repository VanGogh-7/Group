use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use group_agent_model::{ToolDefinition, ToolName};
use jsonschema::Validator;

use crate::{SchemaViolation, Tool, ToolBehavior, ToolDefinitionError, ToolRegistryError};

pub(crate) struct RegisteredTool {
    pub(crate) tool: Arc<dyn Tool>,
    pub(crate) definition: ToolDefinition,
    pub(crate) behavior: ToolBehavior,
    pub(crate) validator: Validator,
}

/// Mutable construction boundary for a deterministic immutable registry.
#[derive(Default)]
pub struct ToolRegistryBuilder {
    entries: BTreeMap<ToolName, RegisteredTool>,
    schema_compilation_count: usize,
}

impl fmt::Debug for ToolRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistryBuilder")
            .field("tool_names", &self.entries.keys().collect::<Vec<_>>())
            .field("schema_compilation_count", &self.schema_compilation_count)
            .finish()
    }
}

impl ToolRegistryBuilder {
    /// Creates an empty registry builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            schema_compilation_count: 0,
        }
    }

    /// Registers one concrete tool.
    pub fn register<T>(&mut self, tool: T) -> Result<&mut Self, ToolRegistryError>
    where
        T: Tool + 'static,
    {
        self.register_arc(Arc::new(tool))
    }

    /// Registers one shared trait object.
    pub fn register_arc(&mut self, tool: Arc<dyn Tool>) -> Result<&mut Self, ToolRegistryError> {
        let advertised = tool.name().clone();
        let definition = tool.definition().clone();
        let defined = definition.name().clone();

        if self.entries.contains_key(&advertised) {
            return Err(ToolRegistryError::DuplicateTool {
                tool_name: advertised,
            });
        }
        if advertised != defined {
            return Err(invalid_definition(
                advertised.clone(),
                ToolDefinitionError::NameMismatch {
                    advertised,
                    defined,
                },
            ));
        }
        if !is_canonical_name(advertised.as_str()) {
            return Err(invalid_definition(
                advertised,
                ToolDefinitionError::InvalidName,
            ));
        }
        if definition.description().trim().is_empty() {
            return Err(invalid_definition(
                advertised,
                ToolDefinitionError::EmptyDescription,
            ));
        }

        let behavior = tool.behavior();
        behavior.validate().map_err(|source| {
            invalid_definition(
                advertised.clone(),
                ToolDefinitionError::InvalidBehavior { source },
            )
        })?;

        self.schema_compilation_count += 1;
        let validator = jsonschema::validator_for(definition.input_schema()).map_err(|source| {
            let violation = SchemaViolation::from_error(&source);
            invalid_definition(
                advertised.clone(),
                ToolDefinitionError::InvalidSchema { violation, source },
            )
        })?;

        self.entries.insert(
            advertised,
            RegisteredTool {
                tool,
                definition,
                behavior,
                validator,
            },
        );
        Ok(self)
    }

    /// Freezes the registry into a cheaply shared, read-only value.
    #[must_use]
    pub fn build(self) -> ToolRegistry {
        let entries = self.entries.into_values().collect::<Vec<_>>();
        let indexes = entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.definition.name().clone(), index))
            .collect();
        ToolRegistry {
            inner: Arc::new(RegistryInner {
                entries: entries.into_boxed_slice(),
                indexes,
                schema_compilation_count: self.schema_compilation_count,
            }),
        }
    }

    /// Returns the number of actual JSON Schema compiler calls made so far.
    ///
    /// Failed schema compilations count; validation failures detected before
    /// the compiler is called do not.
    #[must_use]
    pub const fn schema_compilation_count(&self) -> usize {
        self.schema_compilation_count
    }
}

fn invalid_definition(tool_name: ToolName, source: ToolDefinitionError) -> ToolRegistryError {
    ToolRegistryError::InvalidDefinition { tool_name, source }
}

fn is_canonical_name(name: &str) -> bool {
    name.trim() == name && !name.chars().any(char::is_control)
}

struct RegistryInner {
    entries: Box<[RegisteredTool]>,
    indexes: HashMap<ToolName, usize>,
    schema_compilation_count: usize,
}

/// An immutable, deterministic, cheaply cloned tool registry.
#[derive(Clone)]
pub struct ToolRegistry {
    inner: Arc<RegistryInner>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn empty() -> Self {
        ToolRegistryBuilder::new().build()
    }

    /// Creates a mutable builder.
    #[must_use]
    pub const fn builder() -> ToolRegistryBuilder {
        ToolRegistryBuilder::new()
    }

    /// Registers and freezes an iterator of shared tools.
    pub fn try_from_tools<I>(tools: I) -> Result<Self, ToolRegistryError>
    where
        I: IntoIterator<Item = Arc<dyn Tool>>,
    {
        let mut builder = ToolRegistryBuilder::new();
        for tool in tools {
            builder.register_arc(tool)?;
        }
        Ok(builder.build())
    }

    /// Returns a shared tool by stable name without scanning the registry.
    #[must_use]
    pub fn get(&self, name: &ToolName) -> Option<&Arc<dyn Tool>> {
        self.entry(name).map(|entry| &entry.tool)
    }

    /// Returns cached behavior by stable name.
    #[must_use]
    pub fn behavior(&self, name: &ToolName) -> Option<ToolBehavior> {
        self.entry(name).map(|entry| entry.behavior)
    }

    /// Iterates cached definitions in stable lexical tool-name order.
    pub fn definitions(&self) -> impl ExactSizeIterator<Item = &ToolDefinition> {
        self.inner.entries.iter().map(|entry| &entry.definition)
    }

    /// Returns the number of registered tools.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.entries.len()
    }

    /// Returns whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty()
    }

    /// Returns the number of actual schema compiler calls during registration.
    ///
    /// Execution never changes this count.
    #[must_use]
    pub fn compiled_schema_count(&self) -> usize {
        self.inner.schema_compilation_count
    }

    /// Returns the number of actual schema compiler calls during registration.
    ///
    /// This is equivalent to [`Self::compiled_schema_count`]; the explicit name
    /// distinguishes compiler instrumentation from the number of registry
    /// entries.
    #[must_use]
    pub fn schema_compilation_count(&self) -> usize {
        self.inner.schema_compilation_count
    }

    pub(crate) fn entry(&self, name: &ToolName) -> Option<&RegisteredTool> {
        let index = *self.inner.indexes.get(name)?;
        self.inner.entries.get(index)
    }
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field(
                "tool_names",
                &self
                    .inner
                    .entries
                    .iter()
                    .map(|entry| entry.definition.name())
                    .collect::<Vec<_>>(),
            )
            .field("schema_compilation_count", &self.schema_compilation_count())
            .finish()
    }
}
