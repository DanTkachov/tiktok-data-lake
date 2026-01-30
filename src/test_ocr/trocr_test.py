from transformers import TrOCRProcessor, VisionEncoderDecoderModel
from PIL import Image
import cv2

# Initialize TrOCR model (downloads on first run)
print("Loading TrOCR model...")
processor = TrOCRProcessor.from_pretrained('microsoft/trocr-large-printed')
model = VisionEncoderDecoderModel.from_pretrained('microsoft/trocr-large-printed')
print("Model loaded!\n")

# Read image
img_path = 'img.png'
image = Image.open(img_path).convert("RGB")

# Perform OCR
print("Running OCR...")
pixel_values = processor(images=image, return_tensors="pt").pixel_values
generated_ids = model.generate(pixel_values)
text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]

print(f"\nExtracted text:\n{text}")
