from rapidocr_onnxruntime import RapidOCR
import cv2

# Initialize RapidOCR
engine = RapidOCR()

# Read image
img_path = 'img.png'

# Perform OCR
result, elapse = engine(img_path)

if result:

    all_text = []
    for item in result:
        # RapidOCR returns: [bbox, text, confidence]
        bbox, text, confidence = item[0], item[1], item[2]

        print(f"Text: {text}")
        print(f"Confidence: {confidence:.2f}")
        print(f"Bbox: {bbox}\n")

        # Only include text with good confidence
        if confidence > 0.5:
            all_text.append(text)

    print(f"\nAll text combined:\n{' '.join(all_text)}")
else:
    print("No text detected")
