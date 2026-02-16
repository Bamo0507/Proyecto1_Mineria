{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:

      let
        pkgs = nixpkgs.legacyPackages.${system};

        myR = pkgs.rWrapper.override {
          packages = with pkgs.rPackages; [
            tidyverse
            cluster
            factoextra
            dplyr
            readr
            stringr
            ggplot2
            nortest
            stringi
            GGally
            rmarkdown
            knitr
            yaml
          ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            myR
            pkgs.pandoc
            pkgs.texlive.combined.scheme-full
            pkgs.libintl
            pkgs.cmake

          ];
        };
      }

    );
}
