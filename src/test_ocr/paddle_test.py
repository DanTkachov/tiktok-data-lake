# Disable OneDNN to avoid ConvertPirAttribute2RuntimeAttribute errors
import os
os.environ['FLAGS_use_mkldnn'] = '0'

# Initialize PaddleOCR instance
from paddleocr import PaddleOCR
ocr = PaddleOCR(
    use_angle_cls=True,
    lang='en')

# Run OCR inference on a sample image
result = ocr.predict(
    input="img.png")

# Visualize the results and save the JSON results
for res in result:
    res.print()
    res.save_to_img("output")
    res.save_to_json("output")