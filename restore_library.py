import argparse
import json
import threading
import time
import webbrowser
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import jwt
import requests


DEFAULT_CONFIG_PATH = ".apple_music_restore.json"
DEFAULT_REPORT_PATH = "restore_report.json"
MATCH_THRESHOLD = 0.72
USER_AGENT = "musicdb-library-restorer/0.1"


AUTH_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Apple Music Restore Auth</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f1ea;
      --panel: #fffaf3;
      --text: #1d1d1b;
      --muted: #6b665f;
      --accent: #d4482c;
      --accent-dark: #8f2915;
      --border: #e6dacb;
    }
    body {
      margin: 0;
      font-family: Georgia, "Iowan Old Style", serif;
      background:
        radial-gradient(circle at top right, rgba(212,72,44,.12), transparent 30%),
        radial-gradient(circle at bottom left, rgba(28,113,99,.10), transparent 30%),
        var(--bg);
      color: var(--text);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .card {
      width: min(680px, 100%);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 32px;
      box-shadow: 0 20px 50px rgba(57, 41, 24, .08);
    }
    h1 { margin-top: 0; font-size: 2rem; }
    p { line-height: 1.5; }
    button {
      appearance: none;
      border: 0;
      border-radius: 999px;
      background: var(--accent);
      color: white;
      padding: 14px 22px;
      font-size: 1rem;
      cursor: pointer;
    }
    button:hover { background: var(--accent-dark); }
    .muted { color: var(--muted); }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      background: #f2e9dc;
      border-radius: 16px;
      padding: 16px;
      font-size: 0.95rem;
    }
  </style>
</head>
<body>
  <div class="card">
    <h1>Apple Music Restore</h1>
    <p>Authorize access to your Apple Music library. This will generate a Music User Token in your browser and send it back to the local restore tool.</p>
    <p class="muted">Signed in with the Apple ID that owns the target music library before continuing.</p>
    <button id="authorize">Authorize Apple Music</button>
    <p id="status" class="muted">Waiting for authorization…</p>
    <pre id="details"></pre>
  </div>
  <script src="https://js-cdn.music.apple.com/musickit/v3/musickit.js"></script>
  <script>
    const developerToken = "__DEVELOPER_TOKEN__";
    const appName = "__APP_NAME__";
    const appBuild = "__APP_BUILD__";
    const status = document.getElementById("status");
    const details = document.getElementById("details");
    const button = document.getElementById("authorize");

    async function setup() {
      await MusicKit.configure({
        developerToken,
        app: { name: appName, build: appBuild }
      });
    }

    async function authorize() {
      try {
        status.textContent = "Opening Apple Music authorization…";
        const music = MusicKit.getInstance();
        const musicUserToken = await music.authorize();
        details.textContent = "Music User Token received.";
        const response = await fetch("/token", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ musicUserToken })
        });
        const payload = await response.json();
        status.textContent = payload.message;
      } catch (error) {
        status.textContent = "Authorization failed.";
        details.textContent = String(error);
      }
    }

    button.addEventListener("click", authorize);
    setup().catch((error) => {
      status.textContent = "MusicKit initialization failed.";
      details.textContent = String(error);
    });
  </script>
</body>
</html>
"""


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    lowered = value.casefold().strip()
    simplified = "".join(character if character.isalnum() or character.isspace() else " " for character in lowered)
    tokens = simplified.split()
    return " ".join(tokens)


def similarity(left: str | None, right: str | None) -> float:
    left_normalized = normalize_text(left)
    right_normalized = normalize_text(right)
    if not left_normalized and not right_normalized:
        return 1.0
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0
    return SequenceMatcher(a=left_normalized, b=right_normalized).ratio()


def duration_score(expected_msec: int | None, actual_msec: int | None) -> float:
    if not expected_msec or not actual_msec:
        return 0.0
    delta = abs(expected_msec - actual_msec)
    if delta > 12_000:
        return 0.0
    return max(0.0, 1.0 - delta / 12_000)


def year_score(expected_year: int | None, release_date: str | None) -> float:
    if not expected_year or not release_date:
        return 0.0
    try:
        actual_year = int(release_date[:4])
    except (ValueError, TypeError):
        return 0.0
    if actual_year == expected_year:
        return 1.0
    if abs(actual_year - expected_year) == 1:
        return 0.5
    return 0.0


def build_search_terms(track: dict[str, Any]) -> list[str]:
    primary = " ".join(part for part in [track.get("name"), track.get("artist")] if part)
    fallback = track.get("name") or ""
    return [term for term in [primary, fallback] if term]


def build_description(track: dict[str, Any]) -> str:
    return " — ".join(part for part in [track.get("artist"), track.get("album")] if part)


@dataclass
class MatchResult:
    track_persistent_id: str
    source_name: str
    source_artist: str
    source_album: str
    matched: bool
    score: float
    catalog_song_id: str | None
    catalog_name: str | None
    catalog_artist: str | None
    catalog_album: str | None
    reason: str


class AppleMusicClient:
    def __init__(self, developer_token: str, user_token: str | None = None):
        self.developer_token = developer_token
        self.user_token = user_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {developer_token}",
            "User-Agent": USER_AGENT,
        })
        if user_token:
            self.session.headers["Music-User-Token"] = user_token

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None,
                 json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path if path.startswith("https://") else f"https://api.music.apple.com{path}"
        response = self.session.request(method, url, params=params, json=json_body, timeout=60)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {url} failed: {response.status_code} {response.text}")
        if response.status_code == 204 or not response.content:
            return {}
        return response.json()

    def get_user_storefront(self) -> str:
        payload = self._request("GET", "/v1/me/storefront")
        return payload["data"][0]["id"]

    def search_song_candidates(self, storefront: str, term: str, limit: int = 10) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"/v1/catalog/{storefront}/search",
            params={"term": term, "types": "songs", "limit": limit},
        )
        return payload.get("results", {}).get("songs", {}).get("data", [])

    def get_all_library_songs(self) -> list[dict[str, Any]]:
        songs: list[dict[str, Any]] = []
        next_path = "/v1/me/library/songs?limit=100&include=catalog"
        while next_path:
            payload = self._request("GET", next_path)
            songs.extend(payload.get("data", []))
            next_path = payload.get("next")
        return songs

    def add_songs_to_library(self, catalog_song_ids: list[str]) -> None:
        for batch in chunked(catalog_song_ids, 100):
            self._request("POST", "/v1/me/library", params={"ids[songs]": ",".join(batch)})

    def create_library_playlist(self, name: str, description: str | None = None) -> str:
        attributes: dict[str, Any] = {"name": name}
        if description:
            attributes["description"] = description
        payload = self._request(
            "POST",
            "/v1/me/library/playlists",
            json_body={"attributes": attributes},
        )
        return payload["data"][0]["id"]

    def add_tracks_to_playlist(self, playlist_id: str, library_song_ids: list[str]) -> None:
        for batch in chunked(library_song_ids, 100):
            self._request(
                "POST",
                f"/v1/me/library/playlists/{playlist_id}/tracks",
                json_body={"data": [{"id": song_id, "type": "library-songs"} for song_id in batch]},
            )


class TokenCaptureServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], developer_token: str, app_name: str, app_build: str):
        super().__init__(address, TokenCaptureHandler)
        self.developer_token = developer_token
        self.app_name = app_name
        self.app_build = app_build
        self.music_user_token: str | None = None
        self.event = threading.Event()


class TokenCaptureHandler(BaseHTTPRequestHandler):
    server: TokenCaptureServer

    def do_GET(self) -> None:
        if self.path not in {"/", "/index.html"}:
            self.send_error(404)
            return
        page = (AUTH_PAGE
                .replace("__DEVELOPER_TOKEN__", self.server.developer_token)
                .replace("__APP_NAME__", self.server.app_name)
                .replace("__APP_BUILD__", self.server.app_build))
        body = page.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/token":
            self.send_error(404)
            return
        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length))
        self.server.music_user_token = payload["musicUserToken"]
        self.server.event.set()
        response = json.dumps({"message": "Authorization complete. You can close this tab."}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format_string: str, *args: Any) -> None:
        return


def generate_developer_token(team_id: str, key_id: str, private_key_path: Path, ttl_days: int = 180) -> str:
    now = int(time.time())
    payload = {
        "iss": team_id,
        "iat": now,
        "exp": now + min(ttl_days, 180) * 24 * 60 * 60,
    }
    headers = {"alg": "ES256", "kid": key_id}
    private_key = private_key_path.read_text(encoding="utf-8")
    token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
    return token if isinstance(token, str) else token.decode("utf-8")


def run_auth_flow(config_path: Path, team_id: str, key_id: str, private_key_path: Path,
                  app_name: str, app_build: str, host: str, port: int, no_browser: bool) -> None:
    developer_token = generate_developer_token(team_id, key_id, private_key_path)
    server = TokenCaptureServer((host, port), developer_token, app_name, app_build)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    auth_url = f"http://{host}:{port}/"
    print(f"Open this URL to authorize Apple Music: {auth_url}")
    if not no_browser:
        webbrowser.open(auth_url)

    try:
        if not server.event.wait(timeout=600):
            raise TimeoutError("Timed out waiting for Apple Music authorization.")
    finally:
        server.shutdown()
        server.server_close()

    client = AppleMusicClient(developer_token, server.music_user_token)
    storefront = client.get_user_storefront()
    config = {
        "team_id": team_id,
        "key_id": key_id,
        "private_key_path": str(private_key_path),
        "app_name": app_name,
        "app_build": app_build,
        "music_user_token": server.music_user_token,
        "storefront": storefront,
        "updated_at": int(time.time()),
    }
    save_json(config_path, config)
    print(f"Saved Apple Music credentials to {config_path}")
    print(f"Detected storefront: {storefront}")


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}. Run the auth command first.")
    return load_json(config_path)


def build_client_from_config(config_path: Path) -> tuple[AppleMusicClient, dict[str, Any]]:
    config = load_config(config_path)
    developer_token = generate_developer_token(
        config["team_id"],
        config["key_id"],
        Path(config["private_key_path"]),
    )
    client = AppleMusicClient(developer_token, config["music_user_token"])
    return client, config


def score_candidate(source_track: dict[str, Any], candidate: dict[str, Any]) -> float:
    attributes = candidate.get("attributes", {})
    name_component = similarity(source_track.get("name"), attributes.get("name"))
    artist_component = similarity(source_track.get("artist"), attributes.get("artistName"))
    album_component = similarity(source_track.get("album"), attributes.get("albumName"))
    duration_component = duration_score(source_track.get("total_time"), attributes.get("durationInMillis"))
    year_component = year_score(source_track.get("track_year"), attributes.get("releaseDate"))

    score = (
        name_component * 0.42 +
        artist_component * 0.28 +
        album_component * 0.15 +
        duration_component * 0.10 +
        year_component * 0.05
    )

    if name_component == 1.0 and artist_component > 0.93:
        score += 0.08
    if duration_component == 0.0 and source_track.get("total_time"):
        score -= 0.07
    return max(0.0, min(score, 1.0))


def select_best_match(source_track: dict[str, Any], candidates: list[dict[str, Any]]) -> MatchResult:
    best_candidate = None
    best_score = -1.0
    for candidate in candidates:
        candidate_score = score_candidate(source_track, candidate)
        if candidate_score > best_score:
            best_candidate = candidate
            best_score = candidate_score

    if best_candidate is None:
        return MatchResult(
            track_persistent_id=source_track["track_persistent_id"],
            source_name=source_track.get("name", ""),
            source_artist=source_track.get("artist", ""),
            source_album=source_track.get("album", ""),
            matched=False,
            score=0.0,
            catalog_song_id=None,
            catalog_name=None,
            catalog_artist=None,
            catalog_album=None,
            reason="no_search_results",
        )

    attributes = best_candidate.get("attributes", {})
    matched = best_score >= MATCH_THRESHOLD
    return MatchResult(
        track_persistent_id=source_track["track_persistent_id"],
        source_name=source_track.get("name", ""),
        source_artist=source_track.get("artist", ""),
        source_album=source_track.get("album", ""),
        matched=matched,
        score=round(best_score, 4),
        catalog_song_id=best_candidate.get("id") if matched else None,
        catalog_name=attributes.get("name"),
        catalog_artist=attributes.get("artistName"),
        catalog_album=attributes.get("albumName"),
        reason="matched" if matched else "score_below_threshold",
    )


def match_tracks(client: AppleMusicClient, storefront: str, library_data: dict[str, Any],
                 search_limit: int, delay_seconds: float) -> list[MatchResult]:
    results: list[MatchResult] = []
    tracks = library_data.get("track_data", {}).get("tracks", [])
    total = len(tracks)
    for index, track in enumerate(tracks, start=1):
        candidates: list[dict[str, Any]] = []
        for term in build_search_terms(track):
            candidates = client.search_song_candidates(storefront, term, limit=search_limit)
            if candidates:
                break
        result = select_best_match(track, candidates)
        results.append(result)
        if index % 25 == 0 or index == total:
            print(f"Matched {index}/{total} tracks")
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return results


def build_existing_song_indexes(library_songs: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
    catalog_to_library: dict[str, str] = {}
    fallback_to_library: dict[str, str] = {}
    for item in library_songs:
        library_id = item.get("id")
        attributes = item.get("attributes", {})
        catalog_id = item.get("attributes", {}).get("playParams", {}).get("catalogId")
        if library_id and catalog_id:
            catalog_to_library[catalog_id] = library_id
        fallback_key = "|".join([
            normalize_text(attributes.get("name")),
            normalize_text(attributes.get("artistName")),
            normalize_text(attributes.get("albumName")),
        ])
        if library_id and fallback_key != "||":
            fallback_to_library[fallback_key] = library_id
    return catalog_to_library, fallback_to_library


SYSTEM_PLAYLIST_NAMES = {
    "music",
    "музыка",
    "music videos",
    "видеоклипы",
    "downloaded",
    "загружено",
    "tv & movies",
    "тв и фильмы",
    "favorites songs",
    "любимые песни",
    "genius",
    "shazam tracks",
    "песни из shazam",
    "my shazam tracks",
    "мои записи shazam",
}


def is_probably_system_playlist(playlist_name: str) -> bool:
    return normalize_text(playlist_name) in SYSTEM_PLAYLIST_NAMES


def summarize_matches(matches: list[MatchResult]) -> dict[str, Any]:
    matched = [item for item in matches if item.matched]
    unmatched = [item for item in matches if not item.matched]
    average_score = sum(item.score for item in matched) / len(matched) if matched else 0.0
    return {
        "total_tracks": len(matches),
        "matched_tracks": len(matched),
        "unmatched_tracks": len(unmatched),
        "match_rate": round(len(matched) / len(matches), 4) if matches else 0.0,
        "average_match_score": round(average_score, 4),
    }


def write_report(report_path: Path, library_data: dict[str, Any], matches: list[MatchResult]) -> None:
    match_map = {match.track_persistent_id: match for match in matches}
    playlist_report = []
    for playlist in library_data.get("playlist_data", {}).get("playlists", []):
        matched_tracks = 0
        unmatched_tracks = 0
        for track_ref in playlist.get("tracks", []):
            match = match_map.get(track_ref["track_id"])
            if match and match.matched:
                matched_tracks += 1
            else:
                unmatched_tracks += 1
        playlist_report.append({
            "playlist_id": playlist.get("playlist_id"),
            "name": playlist.get("name"),
            "track_count": playlist.get("track_count", 0),
            "matched_tracks": matched_tracks,
            "unmatched_tracks": unmatched_tracks,
            "skipped_as_system_playlist": is_probably_system_playlist(playlist.get("name", "")),
        })

    report = {
        "summary": summarize_matches(matches),
        "matches": [asdict(match) for match in matches],
        "playlists": playlist_report,
    }
    save_json(report_path, report)


def plan_restore(args: argparse.Namespace) -> None:
    client, config = build_client_from_config(Path(args.config_file))
    library_data = load_json(Path(args.library_json))
    storefront = config.get("storefront") or client.get_user_storefront()
    matches = match_tracks(client, storefront, library_data, args.search_limit, args.request_delay)
    write_report(Path(args.report_file), library_data, matches)
    summary = summarize_matches(matches)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Detailed report written to {args.report_file}")


def restore_library(args: argparse.Namespace) -> None:
    client, config = build_client_from_config(Path(args.config_file))
    library_data = load_json(Path(args.library_json))
    storefront = config.get("storefront") or client.get_user_storefront()

    matches = match_tracks(client, storefront, library_data, args.search_limit, args.request_delay)
    write_report(Path(args.report_file), library_data, matches)

    matched_results = [result for result in matches if result.matched and result.catalog_song_id]
    if not matched_results:
        raise RuntimeError("No matched tracks were found. Inspect the report before retrying.")

    existing_songs = client.get_all_library_songs()
    catalog_to_library, fallback_to_library = build_existing_song_indexes(existing_songs)

    catalog_song_ids_to_add = [
        result.catalog_song_id
        for result in matched_results
        if result.catalog_song_id not in catalog_to_library
    ]
    print(f"Matched tracks: {len(matched_results)}")
    print(f"Already in library: {len(matched_results) - len(catalog_song_ids_to_add)}")
    print(f"Will add to library: {len(catalog_song_ids_to_add)}")

    if not args.dry_run and catalog_song_ids_to_add:
        client.add_songs_to_library(catalog_song_ids_to_add)
        print("Added matched songs to library")
        time.sleep(args.library_refresh_wait)
        existing_songs = client.get_all_library_songs()
        catalog_to_library, fallback_to_library = build_existing_song_indexes(existing_songs)

    match_by_track_id = {result.track_persistent_id: result for result in matches}

    restored_playlists = 0
    for playlist in library_data.get("playlist_data", {}).get("playlists", []):
        playlist_name = playlist.get("name", "").strip()
        if not playlist_name:
            continue
        if not args.include_system_playlists and is_probably_system_playlist(playlist_name):
            continue
        if playlist.get("track_count", 0) == 0 and not args.include_empty_playlists:
            continue

        library_song_ids: list[str] = []
        for track_ref in playlist.get("tracks", []):
            match = match_by_track_id.get(track_ref["track_id"])
            if not match or not match.matched:
                continue
            library_song_id = catalog_to_library.get(match.catalog_song_id or "")
            if not library_song_id:
                fallback_key = "|".join([
                    normalize_text(match.catalog_name),
                    normalize_text(match.catalog_artist),
                    normalize_text(match.catalog_album),
                ])
                library_song_id = fallback_to_library.get(fallback_key)
            if library_song_id:
                library_song_ids.append(library_song_id)

        if args.dry_run:
            print(f"Playlist preview: {playlist_name} -> {len(library_song_ids)} tracks")
            continue

        playlist_id = client.create_library_playlist(playlist_name)
        if library_song_ids:
            client.add_tracks_to_playlist(playlist_id, library_song_ids)
        restored_playlists += 1
        print(f"Restored playlist: {playlist_name} ({len(library_song_ids)} tracks)")

    print(f"Restore complete. Report written to {args.report_file}")
    print(f"Playlists created: {restored_playlists}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore an Apple Music library from library.json via Apple Music API.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_parser = subparsers.add_parser("auth", help="Generate a developer token and capture a Music User Token.")
    auth_parser.add_argument("--team-id", required=True, help="Apple Developer Team ID.")
    auth_parser.add_argument("--key-id", required=True, help="MusicKit key ID.")
    auth_parser.add_argument("--private-key-file", required=True, help="Path to the MusicKit .p8 private key.")
    auth_parser.add_argument("--app-name", default="Music Library Restore", help="Displayed app name during MusicKit auth.")
    auth_parser.add_argument("--app-build", default="1.0", help="Displayed app build during MusicKit auth.")
    auth_parser.add_argument("--host", default="127.0.0.1", help="Local host for the auth page.")
    auth_parser.add_argument("--port", type=int, default=8765, help="Local port for the auth page.")
    auth_parser.add_argument("--config-file", default=DEFAULT_CONFIG_PATH, help="Where to store captured credentials.")
    auth_parser.add_argument("--no-browser", action="store_true", help="Print the URL instead of opening a browser automatically.")

    for command_name in ["plan", "restore"]:
        restore_parser = subparsers.add_parser(command_name, help=f"{command_name.title()} tracks and playlists from library.json.")
        restore_parser.add_argument("library_json", help="Path to the extracted library.json file.")
        restore_parser.add_argument("--config-file", default=DEFAULT_CONFIG_PATH, help="Credentials created by the auth command.")
        restore_parser.add_argument("--report-file", default=DEFAULT_REPORT_PATH, help="Where to write the match report.")
        restore_parser.add_argument("--search-limit", type=int, default=10, help="Candidate search result count per track.")
        restore_parser.add_argument("--request-delay", type=float, default=0.15, help="Delay between search requests in seconds.")
        if command_name == "restore":
            restore_parser.add_argument("--dry-run", action="store_true", help="Do not mutate the Apple Music library.")
            restore_parser.add_argument("--include-system-playlists", action="store_true", help="Also recreate likely Apple-generated playlists.")
            restore_parser.add_argument("--include-empty-playlists", action="store_true", help="Also create empty playlists.")
            restore_parser.add_argument("--library-refresh-wait", type=float, default=5.0, help="Wait after adding songs before refreshing library IDs.")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "auth":
        run_auth_flow(
            Path(args.config_file),
            args.team_id,
            args.key_id,
            Path(args.private_key_file),
            args.app_name,
            args.app_build,
            args.host,
            args.port,
            args.no_browser,
        )
        return
    if args.command == "plan":
        plan_restore(args)
        return
    if args.command == "restore":
        restore_library(args)
        return
    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
