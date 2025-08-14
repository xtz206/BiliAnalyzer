# BiliAnalyzer

A Python-Implemented Command Line Bilibili Comment Fetch and Analysis Tool

## Install

``` shell
git clone https://github.com/XTZ206/BiliAnalyzer.git
cd BiliAnalyzer
uv sync
```

## Usage

### Login With Cookies

``` shell
uv run -m bilianalyzer auth login
# Follow the instructions to login with cookies
# You can get the cookies from your browser
```

### Check Login Status

``` shell
uv run -m bilianalyzer auth status
# Check if you are logged in
```

### Fetch Comments

``` shell
uv run -m bilianalyzer fetch <bvid> [--raw]
# <bvid> is the Bilibili video ID, e.g. BV1xxxx
```

### Analyze Comments

``` shell
uv run -m bilianalyzer analyze <bvid>
```
