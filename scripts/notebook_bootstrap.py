from __future__ import annotations

from textwrap import dedent


DEFAULT_REQUIRED_PACKAGES = ["duckdb", "pandas", "plotly", "pyproj", "shapely"]


def build_runtime_bootstrap_source(
    *,
    output_subdir: str,
    required_packages: list[str] | None = None,
    extra_imports: list[str] | None = None,
) -> str:
    packages = required_packages or DEFAULT_REQUIRED_PACKAGES
    extra_import_block = "\n".join(extra_imports or [])
    if extra_import_block:
        extra_import_block += "\n\n"

    return dedent(
        f"""
        from __future__ import annotations

        {extra_import_block}import importlib.util
        import json
        import os
        import subprocess
        import sys
        from pathlib import Path


        OUTPUT_SUBDIR = {output_subdir!r}
        NOTEBOOK_MODE = os.environ.get("MONOLITHFARM_NOTEBOOK_MODE", "auto").strip().lower()
        if NOTEBOOK_MODE not in {{"auto", "jupyter", "colab"}}:
            raise ValueError("MONOLITHFARM_NOTEBOOK_MODE deve ser `auto`, `jupyter` ou `colab`.")

        PROFILE_NAME = os.environ.get("MONOLITHFARM_PROFILE", "").strip()
        CONFIG_ENV_PATH = os.environ.get("MONOLITHFARM_PATHS_FILE", "").strip()
        IN_COLAB_RUNTIME = "google.colab" in sys.modules
        USE_COLAB_MODE = NOTEBOOK_MODE == "colab" or (NOTEBOOK_MODE == "auto" and IN_COLAB_RUNTIME)
        REQUIRED_PACKAGES = {packages!r}
        COLAB_PROJECT_HINTS = [
            "/content/drive/MyDrive/MonolithFarm",
            "/content/drive/My Drive/MonolithFarm",
            "/content/Projeto-FarmLab",
            "/content/MonolithFarm",
        ]
        COLAB_DATA_HINTS = [f"{{project_dir}}/data" for project_dir in COLAB_PROJECT_HINTS]


        def package_available(name: str) -> bool:
            return importlib.util.find_spec(name) is not None


        def first_existing_path(candidates: list[str]) -> Path | None:
            for candidate in candidates:
                path = Path(candidate).expanduser()
                if path.exists():
                    return path.resolve()
            return None


        def discover_paths_config_file() -> Path | None:
            candidates: list[Path] = []
            if CONFIG_ENV_PATH:
                candidates.append(Path(CONFIG_ENV_PATH).expanduser())

            current = Path.cwd().resolve()
            for candidate in [current, *current.parents]:
                candidates.append(candidate / ".monolithfarm.paths.json")
                candidates.append(candidate / "monolithfarm.paths.json")

            seen: set[str] = set()
            for candidate in candidates:
                resolved = candidate.resolve() if candidate.is_absolute() else candidate
                key = str(resolved)
                if key in seen:
                    continue
                seen.add(key)
                if resolved.exists():
                    return resolved
            return None


        def load_paths_config() -> tuple[dict, Path | None]:
            config_path = discover_paths_config_file()
            if config_path is None:
                return {{}}, None

            payload = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError(f"Arquivo de configuracao invalido: {{config_path}}")
            return payload, config_path


        PATHS_CONFIG, PATHS_CONFIG_FILE = load_paths_config()
        CONFIG_BASE_DIR = PATHS_CONFIG_FILE.parent.resolve() if PATHS_CONFIG_FILE is not None else None
        DEFAULT_PROFILE = str(PATHS_CONFIG.get("default_profile") or "").strip()
        ACTIVE_PROFILE = PROFILE_NAME or DEFAULT_PROFILE
        PROFILE_SETTINGS = {{}}
        if ACTIVE_PROFILE:
            profiles = PATHS_CONFIG.get("profiles", {{}})
            if ACTIVE_PROFILE not in profiles:
                raise KeyError(
                    f"Perfil `{{ACTIVE_PROFILE}}` nao encontrado em {{PATHS_CONFIG_FILE}}."
                )
            profile_value = profiles.get(ACTIVE_PROFILE)
            if isinstance(profile_value, dict):
                PROFILE_SETTINGS = profile_value
        elif isinstance(PATHS_CONFIG.get("profile"), dict):
            PROFILE_SETTINGS = PATHS_CONFIG["profile"]


        def config_value(key: str):
            return PROFILE_SETTINGS.get(key) if isinstance(PROFILE_SETTINGS, dict) else None


        def resolve_config_path(raw_value: object, *, base_dir: Path | None) -> Path | None:
            if raw_value in {{None, ""}}:
                return None
            path = Path(str(raw_value)).expanduser()
            if not path.is_absolute() and base_dir is not None:
                path = (base_dir / path).resolve()
            else:
                path = path.resolve()
            return path


        def mount_colab_drive_if_needed() -> None:
            if not USE_COLAB_MODE:
                return
            if not IN_COLAB_RUNTIME:
                raise RuntimeError(
                    "MONOLITHFARM_NOTEBOOK_MODE='colab' foi definido, mas o runtime atual nao e Google Colab."
                )
            from google.colab import drive

            drive_root = Path("/content/drive")
            drive_root.mkdir(parents=True, exist_ok=True)
            if not (drive_root / "MyDrive").exists() and not (drive_root / "My Drive").exists():
                drive.mount("/content/drive")


        def find_project_dir() -> Path:
            explicit = os.environ.get("MONOLITHFARM_PROJECT_DIR")
            if explicit:
                return Path(explicit).expanduser().resolve()

            config_project_dir = resolve_config_path(config_value("project_dir"), base_dir=CONFIG_BASE_DIR)
            if config_project_dir is not None:
                return config_project_dir

            if USE_COLAB_MODE:
                hinted_project = first_existing_path(COLAB_PROJECT_HINTS)
                if hinted_project is not None and (hinted_project / "pyproject.toml").exists():
                    return hinted_project
                hinted_data = first_existing_path(COLAB_DATA_HINTS)
                if hinted_data is not None and (hinted_data.parent / "pyproject.toml").exists():
                    return hinted_data.parent.resolve()

            current = Path.cwd().resolve()
            for candidate in [current, *current.parents]:
                if (candidate / "pyproject.toml").exists():
                    return candidate

            raise FileNotFoundError(
                "Nao foi possivel localizar `pyproject.toml`. Defina MONOLITHFARM_PROJECT_DIR, "
                "MONOLITHFARM_PROFILE ou crie `.monolithfarm.paths.json`."
            )


        def find_data_dir(project_dir: Path) -> Path:
            explicit = os.environ.get("MONOLITHFARM_DATA_DIR")
            if explicit:
                return Path(explicit).expanduser().resolve()

            config_data_dir = resolve_config_path(config_value("data_dir"), base_dir=CONFIG_BASE_DIR)
            if config_data_dir is not None:
                return config_data_dir

            if USE_COLAB_MODE:
                hinted_data = first_existing_path(COLAB_DATA_HINTS)
                if hinted_data is not None:
                    return hinted_data

            for candidate in [project_dir / "data", project_dir / "FarmLab"]:
                if candidate.exists():
                    return candidate.resolve()

            return (project_dir / "data").resolve()


        def find_output_dir(project_dir: Path) -> Path:
            explicit = os.environ.get("MONOLITHFARM_OUTPUT_DIR")
            if explicit:
                return Path(explicit).expanduser().resolve()

            config_output_dir = resolve_config_path(config_value("output_dir"), base_dir=CONFIG_BASE_DIR)
            if config_output_dir is not None:
                return config_output_dir

            config_output_root = resolve_config_path(config_value("output_root"), base_dir=CONFIG_BASE_DIR)
            if config_output_root is not None:
                return (config_output_root / OUTPUT_SUBDIR).resolve()

            return (project_dir / OUTPUT_SUBDIR).resolve()


        def auto_install_enabled() -> bool:
            explicit = os.environ.get("MONOLITHFARM_AUTO_INSTALL")
            if explicit is not None:
                return explicit == "1"

            config_auto_install = config_value("auto_install")
            if config_auto_install is not None:
                return bool(config_auto_install)

            return USE_COLAB_MODE


        mount_colab_drive_if_needed()
        PROJECT_DIR = find_project_dir()
        DATA_DIR = find_data_dir(PROJECT_DIR)
        OUTPUT_DIR = find_output_dir(PROJECT_DIR)
        AUTO_INSTALL = auto_install_enabled()

        if str(PROJECT_DIR) not in sys.path:
            sys.path.insert(0, str(PROJECT_DIR))

        if AUTO_INSTALL and any(not package_available(name) for name in REQUIRED_PACKAGES):
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-e", str(PROJECT_DIR)])
            except Exception:
                subprocess.check_call(["uv", "pip", "install", "--python", sys.executable, "-e", str(PROJECT_DIR)])

        print("NOTEBOOK_MODE =", NOTEBOOK_MODE)
        print("IN_COLAB_RUNTIME =", IN_COLAB_RUNTIME)
        print("USE_COLAB_MODE =", USE_COLAB_MODE)
        print("PROFILE_NAME =", PROFILE_NAME or "<default>")
        print("PATHS_CONFIG_FILE =", PATHS_CONFIG_FILE)
        print("ACTIVE_PROFILE =", ACTIVE_PROFILE or "<none>")
        print("PROJECT_DIR =", PROJECT_DIR)
        print("DATA_DIR    =", DATA_DIR)
        print("OUTPUT_DIR  =", OUTPUT_DIR)
        print("AUTO_INSTALL =", AUTO_INSTALL)

        if not DATA_DIR.exists():
            raise FileNotFoundError(f"Diretorio de dados nao encontrado: {{DATA_DIR}}")
        """
    ).strip("\n")
