from setuptools import setup, find_packages


setup(name='tfx_extensions_freewheel_preprocessing',
      version='1',
      description='Freewheel Preprocessing Custom TFX Component',
      long_description='Fully custom TFX component to preprocess raw Freewheel logs and create metrics passed '
                       'to the supervised ML model as labels.',
      packages=find_packages(),
      zip_safe=False)
