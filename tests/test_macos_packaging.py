from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = ROOT / "scripts" / "build_macos_app.sh"


def test_macos_app_signing_uses_stable_designated_requirement() -> None:
    script = BUILD_SCRIPT.read_text()

    assert 'BUNDLE_IDENTIFIER="${MUSIC_FETCH_BUNDLE_IDENTIFIER:-local.musicfetch.app}"' in script
    assert 'DEFAULT_CODESIGN_REQUIREMENT="designated => identifier \\"$BUNDLE_IDENTIFIER\\""' in script
    assert '<key>CFBundleIdentifier</key>\n  <string>${BUNDLE_IDENTIFIER}</string>' in script
    assert 'codesign --force --deep --sign - --requirements "=$CODESIGN_REQUIREMENT" "$APP_DIR"' in script


def test_macos_install_keeps_previous_app_backup_until_launch_proof() -> None:
    script = BUILD_SCRIPT.read_text()

    assert 'BACKUP_DIR="$INSTALL_DIR.previous.$(date +%Y%m%d%H%M%S)"' in script
    assert 'Previous app backup: $BACKUP_DIR"' in script
    assert 'rm -rf "$INSTALL_DIR.previous"' not in script
    assert 'mv "$BACKUP_DIR" "$INSTALL_DIR"' in script
