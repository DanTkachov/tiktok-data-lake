"""
Test RapidOCR with different input methods
"""
from rapidocr_onnxruntime import RapidOCR
import numpy as np
from io import BytesIO

# Initialize RapidOCR
engine = RapidOCR()

# Read image file as bytes
img_path = 'img.png'
with open(img_path, 'rb') as f:
    image_bytes = f.read()

print(f"\nTesting 3 methods with {len(image_bytes)} bytes\n")
print("=" * 80)

# ============================================================
# METHOD 1: RAW BYTES directly to RapidOCR
# ============================================================
print("METHOD 1: RAW BYTES (no decoding)")
print("=" * 80)

result_raw, elapse_raw = engine(image_bytes)

if result_raw:
    print(f"Found {len(result_raw)} text regions )\n")
    for i, item in enumerate(result_raw, 1):
        bbox, text, confidence = item[0], item[1], item[2]
        print(f"{i}. '{text}' (confidence: {confidence:.2%})")
else:
    print("No text detected")

print("\n" + "=" * 80)

# ============================================================
# METHOD 2: cv2.imdecode → numpy → RapidOCR
# ============================================================
print("METHOD 2: CV2.IMDECODE")
print("=" * 80)

try:
    import cv2
    img_array = np.frombuffer(image_bytes, dtype=np.uint8)
    img_cv2 = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

    result_cv2, elapse_cv2 = engine(img_cv2)

    if result_cv2:
        print(f"Found {len(result_cv2)} text regions \n")
        for i, item in enumerate(result_cv2, 1):
            bbox, text, confidence = item[0], item[1], item[2]
            print(f"{i}. '{text}' (confidence: {confidence:.2%})")
    else:
        print("No text detected")
except Exception as e:
    print(f"Failed: {e}")

print("\n" + "=" * 80)

# ============================================================
# METHOD 3: PIL → numpy → RapidOCR
# ============================================================
print("METHOD 3: PIL")
print("=" * 80)

try:
    from PIL import Image
    pil_image = Image.open(BytesIO(image_bytes))
    img_pil = np.array(pil_image)

    # Convert RGB to BGR
    if len(img_pil.shape) == 3 and img_pil.shape[2] == 3:
        img_pil = img_pil[:, :, ::-1]

    result_pil, elapse_pil = engine(img_pil)

    if result_pil:
        print(f"Found {len(result_pil)} text regions )\n")
        for i, item in enumerate(result_pil, 1):
            bbox, text, confidence = item[0], item[1], item[2]
            print(f"{i}. '{text}' (confidence: {confidence:.2%})")
    else:
        print("No text detected")
except Exception as e:
    print(f"Failed: {e}")

print("\n" + "=" * 80)

# ============================================================
# COMPARISON
# ============================================================
print("SUMMARY")
print("=" * 80)
print(f"Method 1 (Raw Bytes):  {len(result_raw) if result_raw else 0} regions")
print(f"Method 2 (cv2.imdecode): {len(result_cv2) if 'result_cv2' in locals() and result_cv2 else 0} regions")
print(f"Method 3 (PIL):         {len(result_pil) if 'result_pil' in locals() and result_pil else 0} regions")
print("=" * 80 + "\n")
