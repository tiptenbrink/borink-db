{
  description = "borink-db: light-weight control plane database";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs =
    { nixpkgs }:
    let
      systems = [ "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));

      nativeBuildInputsFor =
        pkgs: with pkgs; [
          cmake
          ninja
          pkg-config
          python3
        ];

      buildInputsFor = pkgs: [ pkgs.libpq ];

      clangdFor =
        pkgs:
        pkgs.writeShellScriptBin "clangd" ''
          exec ${pkgs.llvmPackages_latest.clang-tools}/bin/clangd \
            --query-driver=${pkgs.llvmPackages_latest.stdenv.cc}/bin/clang++ \
            "$@"
        '';

      libpqxxFor =
        pkgs:
        pkgs.llvmPackages_latest.stdenv.mkDerivation {
          pname = "libpqxx";
          version = pkgs.lib.strings.removeSuffix "\n" (builtins.readFile ./libpqxx/VERSION);

          src = pkgs.lib.cleanSource ./libpqxx;

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
            pkg-config
          ];
          buildInputs = [ pkgs.libpq ];

          cmakeFlags = [
            "-DBUILD_SHARED_LIBS=OFF"
            "-DBUILD_DOC=OFF"
            "-DBUILD_EXAMPLES=OFF"
            "-DBUILD_TEST=OFF"
            "-DBUILD_TOOLS=OFF"
            "-DSKIP_BUILD_EXAMPLES=ON"
            "-DSKIP_BUILD_TEST=ON"
          ];

          postInstall = ''
            rm -f "$out/lib/pkgconfig/libpqxx.pc"
          '';
        };

      borinkDbSourceFor =
        pkgs:
        pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            let
              rel = pkgs.lib.removePrefix (toString ./. + "/") (toString path);
            in
            !(
              rel == "libpqxx"
              || pkgs.lib.hasPrefix "libpqxx/" rel
              || rel == "build"
              || pkgs.lib.hasPrefix "build/" rel
              || rel == "result"
            );
        };
    in
    {
      packages = forAllSystems (pkgs: {
        libpqxx = libpqxxFor pkgs;

        default = pkgs.llvmPackages_latest.stdenv.mkDerivation {
          pname = "borink-db";
          version = "0.1.0";

          src = borinkDbSourceFor pkgs;

          nativeBuildInputs = nativeBuildInputsFor pkgs;
          buildInputs = (buildInputsFor pkgs) ++ [ (libpqxxFor pkgs) ];

          cmakeBuildType = "RelWithDebInfo";
          dontStrip = true;
        };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell.override { stdenv = pkgs.llvmPackages_latest.stdenv; } {
          packages =
            (nativeBuildInputsFor pkgs)
            ++ (buildInputsFor pkgs)
            ++ [ (libpqxxFor pkgs) ]
            ++ [
              (clangdFor pkgs)
              pkgs.gdb
              pkgs.nil
              pkgs.nixd
            ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
