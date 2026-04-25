{
  description = "borink-db: light-weight control plane database";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

    libpqxx-src = {
      url = "github:jtv/libpqxx/670daea7900a9b097b9fb199c392b25bb63af48a";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      libpqxx-src,
    }:
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

      clangStdenvFor = pkgs: pkgs.useWildLinker pkgs.llvmPackages_latest.libcxxStdenv;

      clangToolsFor = pkgs: pkgs.llvmPackages_latest.clang-tools.override { enableLibcxx = true; };

      mimallocFor =
        pkgs:
        {
          debug ? false,
        }:
        if debug then
          pkgs.mimalloc.overrideAttrs (old: {
            pname = "mimalloc-debug";
            cmakeBuildType = "Debug";
            dontStrip = true;
            cmakeFlags = (old.cmakeFlags or [ ]) ++ [
              "-DMI_DEBUG_FULL=ON"
              "-DMI_PADDING=ON"
              "-DMI_SHOW_ERRORS=ON"
            ];
          })
        else
          pkgs.mimalloc;

      libpqxxFor =
        pkgs:
        (clangStdenvFor pkgs).mkDerivation {
          pname = "libpqxx";
          version = pkgs.lib.strings.removeSuffix "\n" (builtins.readFile "${libpqxx-src}/VERSION");

          src = libpqxx-src;

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
            pkg-config
            python3
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
              rel == "build"
              || pkgs.lib.hasPrefix "build/" rel
              || rel == "result"
            );
        };

      borinkDbFor =
        pkgs:
        {
          debug ? false,
        }:
        (clangStdenvFor pkgs).mkDerivation {
          pname = "borink-db";
          version = "0.1.0";

          src = borinkDbSourceFor pkgs;

          nativeBuildInputs = nativeBuildInputsFor pkgs;
          buildInputs =
            (buildInputsFor pkgs)
            ++ [
              (libpqxxFor pkgs)
              (mimallocFor pkgs { inherit debug; })
            ];

          cmakeBuildType = if debug then "Debug" else "RelWithDebInfo";
          dontStrip = true;
        };
    in
    {
      packages = forAllSystems (pkgs: {
        libpqxx = libpqxxFor pkgs;
        mimalloc-debug = mimallocFor pkgs { debug = true; };

        release = borinkDbFor pkgs { };
        debug = borinkDbFor pkgs { debug = true; };
        default = borinkDbFor pkgs { };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell.override { stdenv = clangStdenvFor pkgs; } {
          packages =
            (nativeBuildInputsFor pkgs)
            ++ (buildInputsFor pkgs)
            ++ [
              (libpqxxFor pkgs)
            ]
            ++ [
              (clangToolsFor pkgs)
              pkgs.llvmPackages_latest.lldb
              pkgs.nil
              pkgs.nixd
            ];

          BORINK_LIBPQXX_PREFIX = "${libpqxxFor pkgs}";
          BORINK_MIMALLOC_RELEASE_PREFIX = "${(mimallocFor pkgs { }).dev}";
          BORINK_MIMALLOC_DEBUG_PREFIX = "${(mimallocFor pkgs { debug = true; }).dev}";
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
