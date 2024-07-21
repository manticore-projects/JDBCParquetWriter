******************************
How to use it
******************************

Compile from Source Code
==============================

You will need to have ``JDK 8`` or ``JDK 11`` installed.

.. tab:: Maven

    .. code-block:: shell

        git clone https://github.com/manticore-projects/JDBCParquetWriter.git
        cd JDBCParquetWriter
        mvn install

.. tab:: Gradle

    .. code-block:: shell

        git clone https://github.com/manticore-projects/JDBCParquetWriter.git
        cd JDBCParquetWriter
        gradle build



Build Dependencies
==============================

.. tab:: Maven Release

    .. code-block:: xml
        :substitutions:

        <dependency>
            <groupId>com.manticore-projects.jdbc</groupId>
            <artifactId>JDBCParquetWriter</artifactId>
            <version>|JDBCPARQUETWRITER_VERSION|</version>
        </dependency>

.. tab:: Maven Snapshot

    .. code-block:: xml
        :substitutions:

        <repositories>
            <repository>
                <id>sonatype-snapshots</id>
                <snapshots>
                    <enabled>true</enabled>
                </snapshots>
                <url>https://oss.sonatype.org/content/groups/public/</url>
            </repository>
        </repositories>
        <dependency>
            <groupId>com.manticore-projects.jdbc</groupId>
            <artifactId>JDBCParquetWriter</artifactId>
            <version>|JDBCPARQUETWRITER_SNAPSHOT_VERSION|</version>
        </dependency>

.. tab:: Gradle Stable

    .. code-block:: groovy
        :substitutions:

        repositories {
            mavenCentral()
        }

        dependencies {
            implementation 'com.manticore-projects.jdbc:JDBCParquetWriter:|JDBCPARQUETWRITER_VERSION|'
        }

.. tab:: Gradle Snapshot

    .. code-block:: groovy
        :substitutions:

        repositories {
            maven {
                url = uri('https://oss.sonatype.org/content/groups/public/')
            }
        }

        dependencies {
            implementation 'com.manticore-projects.jdbc:JDBCParquetWriter:|JDBCPARQUETWRITER_SNAPSHOT_VERSION|'
        }



