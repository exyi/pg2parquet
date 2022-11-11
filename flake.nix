{
  description = "Command line tool for exporting PostgreSQL tables or queries into Parquet files";
  inputs.nci.url = "github:yusdacra/nix-cargo-integration";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = inputs:
    inputs.nci.lib.makeOutputs {
      root = ./cli;
      systems = inputs.flake-utils.lib.defaultSystems;
      config = common: {
        outputs.defaults = {
          app = "pg2parquet";
          package = "pg2parquet";
        };
      };
      pkgConfig = common: {
        pg2parquet = {
          build = true;
          app = true;
        };
      };
    };
}
