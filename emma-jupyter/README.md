# Emma Jupyter Notebooks

To run the notebooks, you need the following software installed on your machine:

 - the [Jupyter Project](http://jupyter.org/)  itself, and
 - [Jupyter Scala](https://github.com/alexarchambault/jupyter-scala/tree/topic/update-readme) for the Scala kernel.

To see the notebooks, checkout the git code and run the following commands

 ```bash
 cd $path/to/$emma-root
 mvn clean -DskipTests install # install the project
 cd emma-jupyter
 jupyter notebook # run the jupyter notebook
 ```