import time
import pyautogui
from PIL import Image, ImageChops


def image_similarity_percent(img1, img2):
    img1 = img1.convert("RGB")
    img2 = img2.convert("RGB")

    if img1.size != img2.size:
        return 0.0

    diff = ImageChops.difference(img1, img2)
    histogram = diff.histogram()

    r = histogram[0:256]
    g = histogram[256:512]
    b = histogram[512:768]

    diff_score = 0
    for i in range(256):
        diff_score += r[i] * i
        diff_score += g[i] * i
        diff_score += b[i] * i

    max_diff = img1.size[0] * img1.size[1] * 3 * 255
    similarity = 100 - (diff_score / max_diff * 100)

    return max(0, min(100, similarity))


def capture_and_compare(x1, y1, x2, y2, original_img, attempt=1):
    width = x2 - x1
    height = y2 - y1

    if width <= 0 or height <= 0:
        print("Invalid capture area.")
        return 0.0

    captured_img = pyautogui.screenshot(region=(x1, y1, width, height))
    captured_img = captured_img.convert("RGB")

    # optional debug save
    captured_img.save(f"capture_attempt_{attempt}.png")

    similarity = image_similarity_percent(captured_img, original_img)
    return similarity


def main():
    # initial clicks
    x, y = 420, 1060
    x_r, y_r = 90, 60

    # success clicks
    x_c, y_c = 830, 560
    x_m, y_m = 1798, 18   # NEW second click after match

    # capture region
    x1, y1, x2, y2 = 803, 522, 1113, 596

    original_image_path = "old_picture.png"
    similarity_threshold = 99.5

    max_retries = 5
    retry_delay = 2
    post_click_delay = 2   # delay between x_c and x_m

    original_img = Image.open(original_image_path).convert("RGB")

    try:
        # step 1
        pyautogui.click(x, y)
        print(f"Clicked first point: ({x}, {y})")
        time.sleep(3)

        # step 2
        pyautogui.click(x_r, y_r)
        print(f"Clicked second point: ({x_r}, {y_r})")
        time.sleep(3)

        # step 3: retry loop
        for attempt in range(1, max_retries + 1):
            similarity = capture_and_compare(x1, y1, x2, y2, original_img, attempt)

            print(f"[Attempt {attempt}] Similarity: {similarity:.2f}%")

            if similarity >= similarity_threshold:
                print("MATCH")

                # first success click
                pyautogui.click(x_c, y_c)
                print(f"Clicked confirm point: ({x_c}, {y_c})")

                time.sleep(post_click_delay)

                # second success click
                pyautogui.click(x_m, y_m)
                print(f"Clicked final point: ({x_m}, {y_m})")

                return

            if attempt < max_retries:
                print(f"NOT MATCH -> retry after {retry_delay}s")
                time.sleep(retry_delay)

        print("FAILED: No match after all retries.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
