# Experiment with jupyter notebook

WIP

Developing pyspark with jupyter notebooks brings a number of benefits:

1. A interactive dev environment
2. Develop with mocked data, and translating code into package code or tests
3. profile, tune and understand your spark job interactively

## 1. Pre-requisite

- Install jupyter
- Install load spark lib
- Add your virtual environment into your notebook

## 2. Start your Jupiter

run: `jupyter notebook`

### First we need to locate your `pyspark` path

```python
import findspark
findspark.init()# todo code here
```

### Import sibling package from your project:

Depending on where do you run your jupyter notebook you may came across following error. `No module named 'your_package_name'` even though your package are right there. the cause of it could be a very long story.

I will not dive to the details, fortunately the solution is simple:

```python
import sys
sys.path.insert(0,'..')
```

If you would like to understand more how python package import works, checkout this SO post:

[Sibling package imports](https://stackoverflow.com/questions/6323860/sibling-package-imports)

Now, you should be able to use the jupyter note book on your local machine.

See more examples in the code repository:

[zdjohn/spark-setup-workshop/notebook](https://github.com/zdjohn/spark-setup-workshop/tree/master/notebook)

## 3 Tune and profile your code with magic commands

Jupyter notebook magic commands `%time` `%prun` `%memit` (check [https://ipython.readthedocs.io/en/stable/interactive/magics.html](https://ipython.readthedocs.io/en/stable/interactive/magics.html) for more)

## 4. Understand your dataframe and ETL Jobs with following methods

- `.show()`
- `.printSchema()`
- `explain()`
