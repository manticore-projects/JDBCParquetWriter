#######################################################################
Java API
#######################################################################

The API below is generated from the JavaDoc comments of the sources: the ``xmldoc`` Gradle task
runs the `XML Doclet <https://github.com/manticore-projects/xml-doclet>`_ and writes
``_static/javadoc.xml``, which the
`sphinx-javadoc-xml <https://pypi.org/project/sphinx-javadoc-xml/>`_ extension renders here with
cross-links into the OpenJDK JavaDoc.

.. code-block:: shell
    :caption: Regenerate the API description

    gradle xmldoc

..  javadoc-api:: _static/javadoc.xml
    :package: com.manticore.jdbc.parquetwriter
    :public-only: