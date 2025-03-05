# CLAUDE.md - Coding Assistant Reference

## Build Commands
- Build: `cargo build`
- Check: `cargo check`
- Run tests: `cargo test`
- Run single test: `cargo test test_name`
- Run with features: `cargo test --features feature_name`
- Run with release: `cargo build --release`

## Code Style Guidelines
- **Formatting**: Use `cargo fmt` for automatic formatting
- **Linting**: Run `cargo clippy` to find issues
- **Naming**: Use snake_case for variables/functions, CamelCase for types/traits
- **Imports**: Group std imports first, then external crates, then internal modules
- **Error Handling**: Use Result<T, E> with ? operator for propagation
- **Documentation**: Document public APIs with /// comments
- **Tests**: Place tests in `#[cfg(test)]` modules with meaningful names
- **Type Safety**: Prefer strong typing with newtype pattern (like UnitsObjectId)
- **Comments**: Explain "why" not "what" in comments
- **Traits**: Define behavior through traits for better abstraction