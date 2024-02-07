# run terminal code in python with access to file in the same directory
import subprocess

from PIL import Image
from pytesseract import image_to_string


def decode_captcha(filename: str) -> str:
  char_whitelist = 'abcdefghijklmnopqrstuvwxyz0123456789'

  command = f'convert {filename} -colorspace gray -separate -average -threshold 60% -negate -morphology Thinning "Ridges" -negate output.png'

  subprocess.run(command.split(), check=True)

  # read the image
  image = Image.open('output.png')

  # convert image to string
  return str(image_to_string(image, config=f"-c tessedit_char_whitelist={char_whitelist}")).strip()
