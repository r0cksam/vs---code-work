"""
Banner Maker
------------
Reads a photo, a logo, and a caption from a fixed input folder and
composites them into a banner image, saved with a timestamp so older
banners are never overwritten.

INPUT (place these in the "input" folder next to this script/exe):
    input/photo.jpg   - background photo (jpg or png)
    input/logo.png    - logo, ideally a transparent PNG
    input/text.txt    - plain text file with the caption (first line used)

OUTPUT:
    output/banner_YYYY-MM-DD_HHMM.png   - a new file every run, never overwritten

Default layout (placeholder until you confirm exact design):
    - Canvas size: 1920x1080
    - Photo: resized/cropped to fill the whole canvas
    - Logo: top-right corner, 220px wide, with padding
    - Text: centered near the bottom, white with a dark outline for readability

Setup (one-time, on a Windows machine with Python installed):
    pip install pillow pyinstaller

Build a standalone .exe (one-time, so it runs without Python installed):
    pyinstaller --onefile --name banner_maker banner_maker.py
    -> the exe will appear in the "dist" folder; copy it next to your
       "input" folder and double-click it to run.
"""

import os
import sys
import datetime
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

PHOTO_PATH = None  # resolved at runtime, see find_input_file()
LOGO_PATH = None
TEXT_PATH = os.path.join(INPUT_DIR, "text.txt")


def find_input_file(base_name, extensions):
    """Looks for base_name with any of the given extensions (case-insensitive),
    so e.g. a logo saved as .jpg instead of .png still gets picked up."""
    for ext in extensions:
        candidate = os.path.join(INPUT_DIR, f"{base_name}.{ext}")
        if os.path.exists(candidate):
            return candidate
    return os.path.join(INPUT_DIR, f"{base_name}.{extensions[0]}")  # default for error message

CANVAS_SIZE = (1920, 1080)
LOGO_WIDTH = 220
LOGO_PADDING = 40
TEXT_FONT_SIZE = 64
TEXT_PADDING_BOTTOM = 80


def load_font(size):
    # Common Windows font; falls back to PIL's default if not found
    candidates = ["arialbd.ttf", "arial.ttf", "C:\\Windows\\Fonts\\arialbd.ttf"]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def fit_cover(img: Image.Image, target_size):
    """Resize and crop an image to fill target_size, like CSS 'object-fit: cover'."""
    target_w, target_h = target_size
    src_w, src_h = img.size
    scale = max(target_w / src_w, target_h / src_h)
    new_w, new_h = int(src_w * scale), int(src_h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))


def make_banner():
    photo_path = find_input_file("photo", ["jpg", "jpeg", "png"])
    logo_path = find_input_file("logo", ["png", "jpg", "jpeg"])

    missing = [p for p in (photo_path, logo_path, TEXT_PATH) if not os.path.exists(p)]
    if missing:
        print("Missing required input file(s):")
        for p in missing:
            print(f"  - {p}")
        print(f"\nMake sure these exist inside: {INPUT_DIR}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Background photo, filled to canvas
    photo = Image.open(photo_path).convert("RGB")
    canvas = fit_cover(photo, CANVAS_SIZE).convert("RGBA")

    # Logo, top-right corner
    logo = Image.open(logo_path).convert("RGBA")
    logo_ratio = LOGO_WIDTH / logo.width
    logo = logo.resize((LOGO_WIDTH, int(logo.height * logo_ratio)), Image.LANCZOS)
    logo_x = CANVAS_SIZE[0] - logo.width - LOGO_PADDING
    logo_y = LOGO_PADDING
    canvas.paste(logo, (logo_x, logo_y), logo)

    # Caption text, centered near the bottom
    with open(TEXT_PATH, "r", encoding="utf-8") as f:
        caption = f.readline().strip()

    draw = ImageDraw.Draw(canvas)
    font = load_font(TEXT_FONT_SIZE)
    bbox = draw.textbbox((0, 0), caption, font=font)
    text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    text_x = (CANVAS_SIZE[0] - text_w) // 2
    text_y = CANVAS_SIZE[1] - TEXT_PADDING_BOTTOM - text_h

    # simple outline for readability over busy photos
    outline_color = (0, 0, 0, 255)
    fill_color = (255, 255, 255, 255)
    for dx in (-2, 0, 2):
        for dy in (-2, 0, 2):
            if dx != 0 or dy != 0:
                draw.text((text_x + dx, text_y + dy), caption, font=font, fill=outline_color)
    draw.text((text_x, text_y), caption, font=font, fill=fill_color)

    # Save with timestamp, never overwriting
    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"banner_{ts}.png")
    canvas.convert("RGB").save(out_path, "PNG")

    print(f"Banner created: {out_path}")


if __name__ == "__main__":
    try:
        make_banner()
    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        input("\nPress Enter to close...")