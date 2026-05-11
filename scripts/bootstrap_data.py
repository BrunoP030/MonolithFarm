from __future__ import annotations

import argparse
import hashlib
import http.cookiejar
import os
import re
import shutil
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class DataBootstrapError(RuntimeError):
    pass


class DataArchiveAuthenticationError(DataBootstrapError):
    pass


@dataclass(frozen=True)
class BootstrapResult:
    data_dir: Path
    status: str
    message: str


def load_dotenv(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def configured_data_dir(project_root: Path = PROJECT_ROOT) -> Path:
    load_dotenv(project_root / ".env")
    raw = os.environ.get("MONOLITHFARM_DATA_DIR", "data").strip() or "data"
    path = Path(raw)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def ensure_data_dir(data_dir: Path | None = None, *, project_root: Path = PROJECT_ROOT, force: bool = False) -> BootstrapResult:
    load_dotenv(project_root / ".env")
    target = (data_dir or configured_data_dir(project_root)).resolve()
    if target.exists() and any(target.iterdir()) and not force:
        return BootstrapResult(target, "present", "Diretório de dados já existe.")

    archive_url = os.environ.get("MONOLITHFARM_DATA_ARCHIVE_URL", "").strip()
    if not archive_url:
        raise DataBootstrapError(
            "data/ não existe e MONOLITHFARM_DATA_ARCHIVE_URL não está definido no .env local."
        )

    cookie_file = os.environ.get("MONOLITHFARM_DATA_COOKIE_FILE", "").strip()
    expected_sha256 = os.environ.get("MONOLITHFARM_DATA_ARCHIVE_SHA256", "").strip().lower()

    if force and target.exists():
        shutil.rmtree(target)

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="monolithfarm_data_", dir=str(target.parent)) as tmp:
        tmp_dir = Path(tmp)
        archive_path = tmp_dir / "data_archive"
        download_archive(archive_url, archive_path, cookie_file=cookie_file or None)
        if expected_sha256:
            actual = sha256_file(archive_path)
            if actual.lower() != expected_sha256:
                raise DataBootstrapError("Checksum SHA-256 do pacote de dados não confere.")
        extract_dir = tmp_dir / "extract"
        extract_dir.mkdir()
        extract_archive(archive_path, extract_dir)
        candidate = locate_data_root(extract_dir)
        if not candidate:
            raise DataBootstrapError("Pacote baixado não contém uma pasta de dados reconhecível.")
        if target.exists():
            shutil.rmtree(target)
        shutil.move(str(candidate), str(target))

    return BootstrapResult(target, "downloaded", "Diretório de dados baixado e extraído.")


def download_archive(url: str, destination: Path, *, cookie_file: str | None = None) -> None:
    cookie_jar = http.cookiejar.MozillaCookieJar()
    if cookie_file:
        cookie_path = Path(cookie_file).expanduser().resolve()
        if not cookie_path.exists():
            raise DataBootstrapError("Arquivo de cookies configurado não existe.")
        cookie_jar.load(str(cookie_path), ignore_discard=True, ignore_expires=True)
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    request_url = google_drive_download_url(url)

    response = open_request(opener, request_url)
    if is_html_response(response):
        body = response.read(512_000).decode("utf-8", errors="ignore")
        token = google_drive_confirm_token(body, cookie_jar)
        if token:
            response = open_request(opener, google_drive_confirm_url(request_url, body, token))
        else:
            raise_auth_or_html_error(body)

    if is_html_response(response):
        body = response.read(512_000).decode("utf-8", errors="ignore")
        raise_auth_or_html_error(body)

    with destination.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)

    if destination.stat().st_size == 0:
        raise DataBootstrapError("Download do pacote de dados retornou arquivo vazio.")


def probe_archive_access(url: str) -> str:
    cookie_file = os.environ.get("MONOLITHFARM_DATA_COOKIE_FILE", "").strip()
    cookie_jar = http.cookiejar.MozillaCookieJar()
    if cookie_file:
        cookie_jar.load(str(Path(cookie_file).expanduser().resolve()), ignore_discard=True, ignore_expires=True)
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    response = open_request(opener, google_drive_download_url(url))
    if is_html_response(response):
        body = response.read(512_000).decode("utf-8", errors="ignore")
        token = google_drive_confirm_token(body, cookie_jar)
        if token:
            return "download_confirm_required"
        raise_auth_or_html_error(body)
    return "download_available"


def open_request(opener: urllib.request.OpenerDirector, url: str):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "MonolithFarmDataBootstrap/1.0",
            "Accept": "application/octet-stream,text/html;q=0.8,*/*;q=0.5",
        },
    )
    try:
        return opener.open(request, timeout=60)
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise DataArchiveAuthenticationError("Pacote de dados exige autenticação autorizada.") from exc
        raise DataBootstrapError(f"Falha HTTP ao baixar pacote de dados: {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise DataBootstrapError("Falha de rede ao baixar pacote de dados.") from exc


def google_drive_download_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if "drive.google.com" not in parsed.netloc:
        return url
    match = re.search(r"/file/d/([^/]+)", parsed.path)
    file_id = match.group(1) if match else urllib.parse.parse_qs(parsed.query).get("id", [""])[0]
    if not file_id:
        return url
    return f"https://drive.google.com/uc?export=download&id={urllib.parse.quote(file_id)}"


def google_drive_confirm_token(body: str, cookie_jar: http.cookiejar.CookieJar) -> str | None:
    for cookie in cookie_jar:
        if cookie.name.startswith("download_warning"):
            return cookie.value
    patterns = [
        r"confirm=([0-9A-Za-z_\-]+)",
        r'name="confirm"\s+value="([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, body)
        if match:
            return match.group(1)
    return None


def google_drive_confirm_url(original_url: str, body: str, token: str) -> str:
    action_match = re.search(r'<form[^>]+action="([^"]+)"', body, re.IGNORECASE)
    if action_match:
        action = action_match.group(1).replace("&amp;", "&")
        inputs = dict(
            (name, value.replace("&amp;", "&"))
            for name, value in re.findall(r'name="([^"]+)"\s+value="([^"]*)"', body, re.IGNORECASE)
        )
        inputs.setdefault("confirm", token)
        return add_query(action, inputs)
    return add_query(original_url, {"confirm": token})


def is_html_response(response) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    return "text/html" in content_type


def raise_auth_or_html_error(body: str) -> None:
    lower = body.lower()
    if "accounts.google.com" in lower or "signin" in lower or "sign in" in lower or "login" in lower:
        raise DataArchiveAuthenticationError(
            "Pacote de dados exige login no Google Drive; configure MONOLITHFARM_DATA_COOKIE_FILE ou baixe manualmente."
        )
    raise DataBootstrapError("O link configurado não retornou um arquivo compactado de dados.")


def add_query(url: str, params: dict[str, str]) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    for key, value in params.items():
        query[key] = [value]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query, doseq=True)))


def extract_archive(archive: Path, destination: Path) -> None:
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zip_file:
            safe_extract_zip(zip_file, destination)
        return
    if tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tar_file:
            safe_extract_tar(tar_file, destination)
        return
    raise DataBootstrapError("Pacote de dados precisa ser .zip, .tar, .tar.gz ou equivalente.")


def safe_extract_zip(zip_file: zipfile.ZipFile, destination: Path) -> None:
    root = destination.resolve()
    for member in zip_file.infolist():
        target = (destination / member.filename).resolve()
        if not is_relative_to(target, root):
            raise DataBootstrapError("Arquivo compactado contém caminho inseguro.")
    zip_file.extractall(destination)


def safe_extract_tar(tar_file: tarfile.TarFile, destination: Path) -> None:
    root = destination.resolve()
    for member in tar_file.getmembers():
        target = (destination / member.name).resolve()
        if not is_relative_to(target, root):
            raise DataBootstrapError("Arquivo compactado contém caminho inseguro.")
    tar_file.extractall(destination)


def locate_data_root(extract_dir: Path) -> Path | None:
    explicit = extract_dir / "data"
    if explicit.exists() and explicit.is_dir():
        return explicit
    directories = [path for path in extract_dir.iterdir() if path.is_dir()]
    if len(directories) == 1:
        candidate = directories[0]
        nested = candidate / "data"
        if nested.exists() and nested.is_dir():
            return nested
        if looks_like_data_dir(candidate):
            return candidate
    if looks_like_data_dir(extract_dir):
        return extract_dir
    for candidate in directories:
        if looks_like_data_dir(candidate):
            return candidate
    return None


def looks_like_data_dir(path: Path) -> bool:
    names = {child.name.lower() for child in path.iterdir()} if path.exists() else set()
    expected_fragments = ("onesoil", "metos", "ekos", "cropman")
    return any(any(fragment in name for fragment in expected_fragments) for name in names)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Garante a existência local da pasta data/ do MonolithFarm.")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--force", action="store_true", help="Remove e recria o diretório de destino.")
    parser.add_argument("--probe-url", action="store_true", help="Testa se o link configurado responde como download sem extrair.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    load_dotenv()
    try:
        if args.probe_url:
            archive_url = os.environ.get("MONOLITHFARM_DATA_ARCHIVE_URL", "").strip()
            if not archive_url:
                raise DataBootstrapError("MONOLITHFARM_DATA_ARCHIVE_URL não está definido no .env local.")
            status = probe_archive_access(archive_url)
            print(f"Data archive probe: {status}")
            return 0
        result = ensure_data_dir(args.data_dir, force=args.force)
        print(f"{result.message} ({result.data_dir})")
        return 0
    except DataArchiveAuthenticationError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 3
    except DataBootstrapError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
