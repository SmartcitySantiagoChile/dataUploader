import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='data-uploader',
    version='2.0.21',
    author='ADATRAP',
    author_email='cephei.1313@gmail.com',
    description='library to manage ADATRAP data in elasticsearch',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SmartcitySantiagoChile/dataUploader',
    packages=setuptools.find_packages(),
    package_data={
        'datauploader': ['mappings/*.json']
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.0'
)
