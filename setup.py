from setuptools import setup, find_packages

setup(
    name='opal-fetcher-test-dynamodb',  # Unique package name
    version='0.1.0',  # Initial version
    author='R',  # Replace with your name
    author_email=' ',  # Replace with your email
    description='A short description of your package',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/rrrJiia/opal-fetcher-test-dynamoDB',  # Repository URL
    packages=find_packages(),  # Automatically find packages in your directory
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha',  # Indicate the development status
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
    ],
    python_requires='>=3.6',  # Specify compatible Python versions
    install_requires=[
        'boto3',
        'opal-common',
        'cachetools',
        'pydantic',
        'flask',  # Corrected typo from 'flasl' to 'flask'
    ],
    include_package_data=True,  # Include non-code files from MANIFEST.in
)
