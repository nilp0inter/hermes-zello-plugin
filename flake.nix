{
  description = "Zello Channel API platform plugin for Hermes Agent";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    flake-utils,
    nixpkgs,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      lib = nixpkgs.lib;

      # Runtime C libs that opuslib (and ffmpeg) dlopen via ctypes / subprocess.
      runtimeLibs = [pkgs.libopus];

      # Stage the plugin source tree as a Nix-store path that the host's
      # nspawn aspect (modules/aspects/agents/nil-agent-zello.nix in the
      # fleet repo) bind-mounts into the container at
      # ~/.hermes/plugins/zello/.  Python deps are listed in
      # propagatedBuildInputs as a hint for the consumer Python env;
      # actual install happens via hermes-agent's plugin loader.
      pluginTree = pkgs.stdenv.mkDerivation {
        pname = "hermes-zello-plugin-tree";
        version = "0.1.0";
        src = ./.;
        dontBuild = true;
        installPhase = ''
          mkdir -p $out
          cp -r plugin.yaml hermes_zello_plugin $out/
        '';
      };
    in {
      formatter = pkgs.alejandra;

      packages.default = pluginTree;
      packages.plugin-tree = pluginTree;

      devShells.default = pkgs.mkShell {
        packages = [
          pkgs.python3
          pkgs.uv
          pkgs.ffmpeg-headless
          pkgs.libopus
        ];
        LD_LIBRARY_PATH = lib.makeLibraryPath runtimeLibs;
        shellHook = ''
          export LD_LIBRARY_PATH=${lib.makeLibraryPath runtimeLibs}:''${LD_LIBRARY_PATH:-}
        '';
      };
    });
}
