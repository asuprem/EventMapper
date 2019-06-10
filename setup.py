from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='assed',
      version='0.1',
      description='ASSED - Adaptive Social Sensor Event Detection',
      long_description=readme(),
      url='http://github.com/asuprem/ASSED',
      author='Abhijit Suprem',
      author_email='asuprem@gatech.edu',
      license='MIT',
      packages=['assed'],
      install_requires=[
          'flask','click', 'cmd2'
      ],
      python_requires=">=3.5",
      scripts=["bin/assed-cli", "bin/assed-engine", "bin/assed-server", "bin/assed-sh"],
      zip_safe=False)