import easyocr
import cv2

# Initialize EasyOCR reader (downloads models on first run)
reader = easyocr.Reader(['en'])

# Read image
img = cv2.imread('img.png')

# Perform OCR
result = reader.readtext(
    img,
    detail=1,
    paragraph=False,  # Don't merge text
    width_ths=0.7,    # Adjust text box width threshold
    mag_ratio=1.5     # Magnify image for better detection
)

# Print results
print(f"\nFound {len(result)} text regions:\n")
for (bbox, text, confidence) in result:
    print(f"Text: {text}")
    print(f"Confidence: {confidence:.2f}")
    print(f"Bbox: {bbox}\n")

# Extract just the text
all_text = [text for (bbox, text, conf) in result if conf > 0.5]
print(f"\nAll text combined:\n{' '.join(all_text)}")
