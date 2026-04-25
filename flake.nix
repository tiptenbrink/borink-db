{
  description = "borink-db: light-weight control plane database";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

    libpqxx-src = {
      url = "github:jtv/libpqxx/670daea7900a9b097b9fb199c392b25bb63af48a";
      flake = false;
    };

    quickcpplib-src = {
      url = "git+https://github.com/ned14/quickcpplib.git?rev=3c1d8cb5e94722447e4f17e87b5a9e3a0c66fb39&submodules=1";
      flake = false;
    };

    outcome-src = {
      url = "git+https://github.com/ned14/outcome.git?rev=2db94be7aa1c65115d1942042238317d53ab601c&submodules=1";
      flake = false;
    };

    llfio-src = {
      url = "git+https://github.com/ned14/llfio.git?rev=b3c9308f143e27161c40f6d52a8fd18e8f05761b&submodules=1";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      libpqxx-src,
      quickcpplib-src,
      outcome-src,
      llfio-src,
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

      quickcpplibFor =
        pkgs:
        (clangStdenvFor pkgs).mkDerivation {
          pname = "quickcpplib";
          version = "unstable-2026-04-25";

          src = quickcpplib-src;

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
          ];

          cmakeFlags = [
            "-Dquickcpplib_IS_DEPENDENCY=ON"
            "-DQUICKCPPLIB_ENABLE_DOXYGEN_DOCS_BUILD_SUPPORT=OFF"
            "-DBUILD_TESTING=OFF"
            "-DCMAKE_CXX_STANDARD=20"
          ];
        };

      outcomeFor =
        pkgs:
        (clangStdenvFor pkgs).mkDerivation {
          pname = "outcome";
          version = "unstable-2026-04-25";

          src = outcome-src;

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
          ];

          buildInputs = [ (quickcpplibFor pkgs) ];

          cmakeFlags = [
            "-Doutcome_IS_DEPENDENCY=ON"
            "-DOUTCOME_ENABLE_CXX_MODULES=OFF"
            "-DOUTCOME_ENABLE_DEPENDENCY_SMOKE_TEST=OFF"
            "-DBUILD_TESTING=OFF"
            "-DCMAKE_CXX_STANDARD=20"
          ];
        };

      llfioFor =
        pkgs:
        (clangStdenvFor pkgs).mkDerivation {
          pname = "llfio";
          version = "unstable-2026-04-25";

          src = llfio-src;

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
          ];

          buildInputs = [
            (quickcpplibFor pkgs)
            (outcomeFor pkgs)
          ];

          cmakeFlags = [
            "-Dllfio_IS_DEPENDENCY=ON"
            "-DLLFIO_ENABLE_DEPENDENCY_SMOKE_TEST=OFF"
            "-DLLFIO_FORCE_COROUTINES_OFF=ON"
            "-DCXX_COROUTINES_FLAGS="
            "-DBUILD_TESTING=OFF"
            "-DCMAKE_CXX_STANDARD=20"
          ];
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
              (quickcpplibFor pkgs)
              (outcomeFor pkgs)
              (libpqxxFor pkgs)
              (llfioFor pkgs)
              (mimallocFor pkgs { inherit debug; })
            ];

          cmakeBuildType = if debug then "Debug" else "RelWithDebInfo";
          dontStrip = true;
        };
    in
    {
      packages = forAllSystems (pkgs: {
        libpqxx = libpqxxFor pkgs;
        quickcpplib = quickcpplibFor pkgs;
        outcome = outcomeFor pkgs;
        llfio = llfioFor pkgs;
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
              (quickcpplibFor pkgs)
              (outcomeFor pkgs)
              (libpqxxFor pkgs)
              (llfioFor pkgs)
            ]
            ++ [
              (clangToolsFor pkgs)
              pkgs.llvmPackages_latest.lldb
              pkgs.nil
              pkgs.nixd
            ];

          BORINK_QUICKCPPLIB_PREFIX = "${quickcpplibFor pkgs}";
          BORINK_OUTCOME_PREFIX = "${outcomeFor pkgs}";
          BORINK_LIBPQXX_PREFIX = "${libpqxxFor pkgs}";
          BORINK_LLFIO_PREFIX = "${llfioFor pkgs}";
          BORINK_MIMALLOC_RELEASE_PREFIX = "${(mimallocFor pkgs { }).dev}";
          BORINK_MIMALLOC_DEBUG_PREFIX = "${(mimallocFor pkgs { debug = true; }).dev}";
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
