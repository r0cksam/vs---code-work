r"""Create polished news banners from layered PNG artwork.

Required files in ``input`` beside this script:

    BG.png
    character.png
    Text.png
    Lagataar coverage.png
    india tv logo white.png
    Live.png

The script creates 1920x840, 1800x520, and 640x360 PNG/JPG banners.
When matching ``Mi Tv <size>.png`` files exist in ``AUTO creative design``,
it also performs a visual sanity check and prints a similarity score.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import sys
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageChops, ImageEnhance, ImageFilter, ImageStat


try:
    RESAMPLE = Image.Resampling.LANCZOS
    NEAREST = Image.Resampling.NEAREST
except AttributeError:  # Pillow < 9.1
    RESAMPLE = Image.LANCZOS
    NEAREST = Image.NEAREST


def application_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = application_dir()
DEFAULT_INPUT_DIR = BASE_DIR / "input"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_REFERENCE_DIR = BASE_DIR / "AUTO creative design"

ASSET_NAMES = {
    "background": "BG.png",
    "character": "character.png",
    "text": "Text.png",
    "coverage": "Lagataar coverage.png",
    "logo": "india tv logo white.png",
    "live": "Live.png",
}

RECOMMENDED_SIZES = {
    "background": (1920, 840),
    "character": (1149, 840),
    "text": (929, 217),
    "coverage": (302, 134),
    "logo": (172, 47),
    "live": (138, 57),
}

TRANSPARENT_ASSETS = {"character", "text", "coverage", "logo"}


@dataclass(frozen=True)
class CharacterPlacement:
    region: str
    scale: float
    x: int
    y: int
    fill_bottom: bool = True


@dataclass(frozen=True)
class Layout:
    size: tuple[int, int]
    background_mode: str
    background_focus: tuple[float, float]
    shade_strength: float
    shade_center: float
    shade_width: float
    characters: tuple[CharacterPlacement, ...]
    text_width: int
    text_x: int
    text_y: int
    branding: tuple[tuple[str, int, int, int], ...]

    @property
    def label(self) -> str:
        return f"{self.size[0]}x{self.size[1]}"


# These values were measured against the approved reference artwork. Shorter
# canvases need independent person scaling; treating character.png as one block
# makes the faces too large and shifts the visual balance.
LAYOUTS = (
    Layout(
        size=(1920, 840),
        background_mode="cover",
        background_focus=(0.50, 0.50),
        shade_strength=0.24,
        shade_center=0.38,
        shade_width=0.27,
        characters=(CharacterPlacement("all", 1.0, 771, 0),),
        text_width=929,
        text_x=31,
        text_y=160,
        branding=(("coverage", 302, 329, 420),),
    ),
    Layout(
        size=(1800, 520),
        background_mode="cover",
        background_focus=(0.50, 0.85),
        shade_strength=0.31,
        shade_center=0.34,
        shade_width=0.28,
        characters=(
            CharacterPlacement("left", 0.65764, 910, -50),
            CharacterPlacement("right", 0.64735, 927, -32),
        ),
        text_width=869,
        text_x=61,
        text_y=65,
        branding=(("coverage", 291, 350, 300),),
    ),
    Layout(
        size=(640, 360),
        background_mode="stretch",
        background_focus=(0.29, 0.50),
        shade_strength=0.34,
        shade_center=0.22,
        shade_width=0.30,
        characters=(
            CharacterPlacement("left", 0.34130, 241, 50),
            CharacterPlacement("right", 0.38487, 195, 41),
        ),
        text_width=289,
        text_x=17,
        text_y=82,
        branding=(
            ("logo", 172, 76, 167),
            ("live", 138, 80, 235),
        ),
    ),
)


@dataclass(frozen=True)
class QualityCheck:
    label: str
    reference: Path | None
    similarity: float | None
    passed: bool
    message: str


def find_file(directory: Path, wanted_name: str) -> Path:
    if not directory.is_dir():
        raise FileNotFoundError(f"Folder not found: {directory}")
    files = {path.name.casefold(): path for path in directory.iterdir() if path.is_file()}
    try:
        return files[wanted_name.casefold()]
    except KeyError as exc:
        raise FileNotFoundError(f'Missing "{wanted_name}" inside: {directory}') from exc


def load_assets(directory: Path) -> dict[str, Image.Image]:
    assets: dict[str, Image.Image] = {}
    for key, filename in ASSET_NAMES.items():
        path = find_file(directory, filename)
        with Image.open(path) as source:
            image = source.convert("RGBA")
            image.load()
        assets[key] = image
    return assets


def validate_assets(assets: dict[str, Image.Image]) -> list[str]:
    warnings: list[str] = []
    for key, expected in RECOMMENDED_SIZES.items():
        actual = assets[key].size
        actual_ratio = actual[0] / actual[1]
        expected_ratio = expected[0] / expected[1]
        if actual[0] < expected[0] or actual[1] < expected[1]:
            warnings.append(
                f"{ASSET_NAMES[key]} is {actual[0]}x{actual[1]}; "
                f"recommended minimum is {expected[0]}x{expected[1]}"
            )
        if abs(actual_ratio / expected_ratio - 1.0) > 0.04:
            warnings.append(
                f"{ASSET_NAMES[key]} has a different aspect ratio; placement may shift"
            )
        if key in TRANSPARENT_ASSETS:
            minimum_alpha, _ = assets[key].getchannel("A").getextrema()
            if minimum_alpha == 255:
                warnings.append(f"{ASSET_NAMES[key]} has no transparent background")
    return warnings


def fit_cover(
    image: Image.Image,
    target_size: tuple[int, int],
    focus: tuple[float, float],
) -> Image.Image:
    target_w, target_h = target_size
    scale = max(target_w / image.width, target_h / image.height)
    resized = image.resize(
        (max(target_w, round(image.width * scale)), max(target_h, round(image.height * scale))),
        RESAMPLE,
    )
    overflow_x = resized.width - target_w
    overflow_y = resized.height - target_h
    left = round(overflow_x * min(1.0, max(0.0, focus[0])))
    top = round(overflow_y * min(1.0, max(0.0, focus[1])))
    return resized.crop((left, top, left + target_w, top + target_h))


def resize_to_width(image: Image.Image, width: int) -> Image.Image:
    height = max(1, round(image.height * width / image.width))
    return image.resize((width, height), RESAMPLE)


def resize_by_scale(image: Image.Image, scale: float) -> Image.Image:
    return image.resize(
        (max(1, round(image.width * scale)), max(1, round(image.height * scale))),
        RESAMPLE,
    )


def extend_lower_body_to_canvas(
    layer: Image.Image, position_y: int, canvas_height: int
) -> Image.Image:
    """Extend only the lower body so a scaled character never ends in a hard line."""
    target_height = canvas_height - position_y
    if target_height <= layer.height:
        return layer
    anchor = round(layer.height * 0.62)
    result = Image.new("RGBA", (layer.width, target_height), (0, 0, 0, 0))
    result.alpha_composite(layer.crop((0, 0, layer.width, anchor)), (0, 0))
    lower = layer.crop((0, anchor, layer.width, layer.height)).resize(
        (layer.width, target_height - anchor), RESAMPLE
    )
    result.alpha_composite(lower, (0, anchor))
    return result


def alpha_composite_clipped(
    canvas: Image.Image, layer: Image.Image, position: tuple[int, int]
) -> None:
    x, y = position
    left = max(0, x)
    top = max(0, y)
    right = min(canvas.width, x + layer.width)
    bottom = min(canvas.height, y + layer.height)
    if right <= left or bottom <= top:
        return
    crop = layer.crop((left - x, top - y, right - x, bottom - y))
    canvas.alpha_composite(crop, (left, top))


def add_readability_shade(
    background: Image.Image,
    strength: float,
    center_ratio: float,
    width_ratio: float,
) -> Image.Image:
    center = background.width * center_ratio
    spread = max(1.0, background.width * width_ratio)
    max_alpha = round(255 * min(1.0, max(0.0, strength)))
    values = []
    for x in range(background.width):
        distance = (x - center) / spread
        values.append(round(max_alpha * math.exp(-2.0 * distance * distance)))
    strip = Image.new("L", (background.width, 1))
    strip.putdata(values)
    alpha = strip.resize(background.size, NEAREST)
    overlay = Image.new("RGBA", background.size, (0, 0, 0, 0))
    overlay.putalpha(alpha)
    return Image.alpha_composite(background, overlay)


def prepare_background(background: Image.Image, layout: Layout) -> Image.Image:
    if layout.background_mode == "stretch":
        canvas = background.resize(layout.size, RESAMPLE).convert("RGBA")
    else:
        canvas = fit_cover(background, layout.size, layout.background_focus).convert("RGBA")
    canvas = ImageEnhance.Color(canvas).enhance(1.03)
    canvas = ImageEnhance.Contrast(canvas).enhance(1.025)
    return add_readability_shade(
        canvas,
        layout.shade_strength,
        layout.shade_center,
        layout.shade_width,
    )


def select_character_region(character: Image.Image, region: str) -> Image.Image:
    if region == "all":
        return character.copy()

    # The two people overlap slightly in the flattened source. These feathered
    # gates retain that overlap while allowing independent scale/position.
    if region == "left":
        start, end = 740, 790
        values = [
            255 if x <= start else 0 if x >= end else round(255 * (end - x) / (end - start))
            for x in range(character.width)
        ]
    elif region == "right":
        start, end = 690, 740
        values = [
            0 if x <= start else 255 if x >= end else round(255 * (x - start) / (end - start))
            for x in range(character.width)
        ]
    else:
        raise ValueError(f"Unknown character region: {region}")

    gate_strip = Image.new("L", (character.width, 1))
    gate_strip.putdata(values)
    gate = gate_strip.resize(character.size, NEAREST)
    result = character.copy()
    result.putalpha(ImageChops.multiply(character.getchannel("A"), gate))
    return result


def add_character_shadow(
    canvas: Image.Image, character: Image.Image, position: tuple[int, int]
) -> None:
    radius = max(4, round(canvas.height * 0.010))
    alpha = character.getchannel("A").filter(ImageFilter.GaussianBlur(radius))
    alpha = alpha.point(lambda value: round(value * 0.22))
    shadow = Image.new("RGBA", character.size, (0, 0, 0, 0))
    shadow.putalpha(alpha)
    x, y = position
    alpha_composite_clipped(canvas, shadow, (x - radius, y + radius // 3))


def render_layout(assets: dict[str, Image.Image], layout: Layout) -> Image.Image:
    canvas = prepare_background(assets["background"], layout)

    for placement in layout.characters:
        source = select_character_region(assets["character"], placement.region)
        character = resize_by_scale(source, placement.scale)
        if placement.fill_bottom:
            character = extend_lower_body_to_canvas(
                character, placement.y, layout.size[1]
            )
        position = (placement.x, placement.y)
        add_character_shadow(canvas, character, position)
        alpha_composite_clipped(canvas, character, position)

    text = resize_to_width(assets["text"], layout.text_width)
    alpha_composite_clipped(canvas, text, (layout.text_x, layout.text_y))

    for asset_key, width, x, y in layout.branding:
        layer = resize_to_width(assets[asset_key], width)
        alpha_composite_clipped(canvas, layer, (x, y))

    return canvas


def find_reference(reference_dir: Path, label: str) -> Path | None:
    if not reference_dir.is_dir():
        return None
    for suffix in ("png", "jpg", "jpeg"):
        wanted = f"Mi Tv {label}.{suffix}".casefold()
        for path in reference_dir.iterdir():
            if path.is_file() and path.name.casefold() == wanted:
                return path
    return None


def compare_with_reference(
    image: Image.Image,
    label: str,
    reference_dir: Path,
    minimum_similarity: float,
) -> QualityCheck:
    reference_path = find_reference(reference_dir, label)
    if reference_path is None:
        return QualityCheck(label, None, None, True, "reference not found; check skipped")

    with Image.open(reference_path) as source:
        reference = source.convert("RGB")
        reference.load()
    if reference.size != image.size:
        return QualityCheck(
            label,
            reference_path,
            0.0,
            False,
            f"size mismatch: generated {image.size}, reference {reference.size}",
        )

    # Downsampling and a slight blur make this a composition check rather than
    # a fragile compression/noise comparison.
    sample_width = min(960, image.width)
    sample_height = round(image.height * sample_width / image.width)
    generated_sample = image.convert("RGB").resize((sample_width, sample_height), RESAMPLE)
    reference_sample = reference.resize((sample_width, sample_height), RESAMPLE)
    generated_sample = generated_sample.filter(ImageFilter.GaussianBlur(1.2))
    reference_sample = reference_sample.filter(ImageFilter.GaussianBlur(1.2))
    difference = ImageChops.difference(generated_sample, reference_sample)
    mean_error = sum(ImageStat.Stat(difference).mean) / 3
    similarity = 100.0 * (1.0 - mean_error / 255.0)
    passed = similarity >= minimum_similarity
    message = (
        f"reference similarity {similarity:.2f}% "
        f"(minimum {minimum_similarity:.2f}%)"
    )
    return QualityCheck(label, reference_path, similarity, passed, message)


def unique_run_id(output_dir: Path) -> str:
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    candidate = timestamp
    counter = 2
    while any(output_dir.glob(f"banner_*_{candidate}.*")):
        candidate = f"{timestamp}_{counter:02d}"
        counter += 1
    return candidate


def save_banner(
    image: Image.Image,
    output_dir: Path,
    label: str,
    run_id: str,
    save_jpeg: bool,
) -> list[Path]:
    stem = f"banner_{label}_{run_id}"
    png_path = output_dir / f"{stem}.png"
    image.save(png_path, "PNG", optimize=True)
    paths = [png_path]
    if save_jpeg:
        jpg_path = output_dir / f"{stem}.jpg"
        image.convert("RGB").save(
            jpg_path, "JPEG", quality=95, subsampling=0, optimize=True
        )
        paths.append(jpg_path)
    return paths


def make_banners(
    input_dir: Path,
    output_dir: Path,
    reference_dir: Path,
    selected_labels: set[str] | None,
    save_jpeg: bool,
    check_references: bool,
    minimum_similarity: float,
) -> tuple[list[Path], list[QualityCheck], list[str]]:
    assets = load_assets(input_dir)
    asset_warnings = validate_assets(assets)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = unique_run_id(output_dir)
    created: list[Path] = []
    checks: list[QualityCheck] = []

    for layout in LAYOUTS:
        if selected_labels and layout.label not in selected_labels:
            continue
        banner = render_layout(assets, layout)
        if check_references:
            checks.append(
                compare_with_reference(
                    banner, layout.label, reference_dir, minimum_similarity
                )
            )
        created.extend(save_banner(banner, output_dir, layout.label, run_id, save_jpeg))
    return created, checks, asset_warnings


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create and sanity-check responsive news banners."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"layered artwork folder (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reference-dir", type=Path, default=DEFAULT_REFERENCE_DIR)
    parser.add_argument(
        "--only",
        action="append",
        choices=[layout.label for layout in LAYOUTS],
        help="render only this size; repeat to select multiple sizes",
    )
    parser.add_argument("--png-only", action="store_true")
    parser.add_argument("--no-reference-check", action="store_true")
    parser.add_argument("--minimum-similarity", type=float, default=80.0)
    parser.add_argument(
        "--strict-check",
        action="store_true",
        help="return an error code if any reference sanity check fails",
    )
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    status = 0
    try:
        created, checks, warnings = make_banners(
            input_dir=args.input_dir.expanduser().resolve(),
            output_dir=args.output_dir.expanduser().resolve(),
            reference_dir=args.reference_dir.expanduser().resolve(),
            selected_labels=set(args.only) if args.only else None,
            save_jpeg=not args.png_only,
            check_references=not args.no_reference_check,
            minimum_similarity=min(100.0, max(0.0, args.minimum_similarity)),
        )
        for warning in warnings:
            print(f"[INPUT WARNING] {warning}")
        for check in checks:
            marker = "PASS" if check.passed else "FAIL"
            print(f"[{marker}] {check.label}: {check.message}")
        print(f"Created {len(created)} file(s):")
        for path in created:
            print(f"  {path}")
        if args.strict_check and any(not check.passed for check in checks):
            status = 2
    except Exception as exc:
        status = 1
        print(f"Banner creation failed: {exc}", file=sys.stderr)

    if not args.no_pause and sys.stdin is not None and sys.stdin.isatty():
        try:
            input("\nPress Enter to close...")
        except (EOFError, KeyboardInterrupt):
            pass
    return status


if __name__ == "__main__":
    raise SystemExit(main())
