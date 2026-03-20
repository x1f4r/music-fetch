class MusicFetch < Formula
  include Language::Python::Virtualenv

  desc "Local-first music recognition for noisy video and playlists"
  homepage "https://example.com/music-fetch"
  url "file:///tmp/music-fetch-0.2.3.tar.gz"
  sha256 "CHANGE_ME"
  license "MIT"

  depends_on "ffmpeg"
  depends_on "yt-dlp"
  depends_on "chromaprint"
  depends_on "uv"
  depends_on "python@3.12"

  def install
    virtualenv_install_with_resources
  end

  test do
    system bin/"music-fetch", "doctor"
  end
end
