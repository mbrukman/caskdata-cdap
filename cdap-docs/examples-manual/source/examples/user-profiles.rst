.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-user-profiles:

=============
User Profiles
=============

A Cask Data Application Platform (CDAP) example demonstrating column-level conflict
detection in Datasets using the example of managing user profiles in a Table.

Overview
========

This application demonstrates the use of the column-level conflict detection in a Dataset,
through the example of an application that manages user profiles in a Table.
The fields of a user profile are updated in different ways:

  - Attributes such as name and email address are changed through a RESTful call when the 
    user updates their profile.
  - The time of the last login is updated by a sign-on service every
    time the user logs in, also through a RESTful call.
  - The time of the last activity is updated by a Flow that processes
    events whenever it encounters an event from that user.

This application illustrates both row-level and column-level conflict detection for a Table.

Let's look at some of these components, and then run the Application and see the results.

Introducing a Feature: Column-Level Conflict Detection
======================================================

As in the other :ref:`examples,<examples-index>` the components
of the Application are tied together by a class ``UserProfiles``:

.. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
    :language: java
    :lines: 31-

This application uses a Table with conflict detection either at the row level or
at the column level.

A conflict occurs if two transactions that overlap in time modify the same data in a table.
For example, a Flowlet's process method might overlap with a Service handler.
Such a conflict is detected at the time that the transactions are committed,
and the transaction that attempts to commit last is rolled back.

By default, the granularity of the conflict detection is at the row-level.
That means it is sufficient for two overlapping transactions writing to
the same row of a table to cause a conflict, even if they write to different
columns.

Specifying a conflict detection level of ``COLUMN`` means that a conflict is
only detected if both transactions modify the same column of the same row.
This is more precise, but it requires more book-keeping in the transaction
system and thus can impact performance.

Column-level conflict detection should be enabled if it is known that different
transactions frequently modify different columns of the same row concurrently.

UserProfiles Application
------------------------

This application uses:

- a Stream ``events`` to receive events of user activity;
- a Dataset ``profiles`` to store user profiles with conflict detection at either the row or column level;
- a Dataset ``counters`` to count events by URL (this is not essential for the purpose of the example);
- a Service ``UserProfileService`` to create, delete, and update profiles; and
- a Flow ``ActivityFlow`` to count events and record the time of last activity for the users.

The ``UserProfileService`` is a Service for creating and modifying user profiles. It has
handlers to create, update, and retrieve user profiles.

A script (``add-users.sh``) is used to populate the ``profiles`` Dataset. Two additional 
scripts (``update-login.sh`` and ``send-events.sh``) are used to create a conflict by attempting
to write to two different columns of the same row at the same time.


Building and Starting
=====================

To observe conflict detection at both the row-level and column-level, you will need to modify 
and build this example twice:

- The first time, you will use row-level conflict detection, and see errors appearing in a log;
- The second time, you will use column-level conflict detection and see the scripts complete successfully without errors.
  
Build the Application with Row-level Conflict Detection
-------------------------------------------------------

Before building the application, set the ``ConflictDetection`` appropriately in the class ``UserProfiles``:

.. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
      :language: java
      :lines: 41-42
      
- The first time you build the application, set the ``tableProperties`` to ``ConflictDetection.ROW``. 

- Build the example (as described `below <#building-an-example-application>`__).
- Start CDAP, deploy and start the application and its component as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow and Service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__
- You should observe errors as described.

Re-build the Application with Column-level Conflict Detection
-------------------------------------------------------------

- Stop the Application's Flow and Service (as described `below <#stopping-the-application>`__).
- Delete the ``profiles`` Dataset, either through the CDAP Command Line Interface or
  by making a ``curl`` call::

    curl -w '\n' -v localhost:10000/v2/data/datasets/profiles -XDELETE

- Now, rebuild the application, setting the ``tableProperties`` back to its original value, ``ConflictDetection.COLUMN``.
- Re-deploy and re-run the application. You should not see any errors now in the log.    

.. **Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
.. SDK in ``libexec\bin\curl.exe``


Running CDAP Applications
============================================

.. include:: /../../developers-manual/build/_includes/building-apps-versioned.rst
    :start-line: 9

Running the Example
===================

Deleting an Existing Dataset
----------------------------

If it has been created from an earlier run of the example, delete the ``profiles``
Dataset, either through the CDAP Command Line Interface or by making a ``curl`` call::

  curl -w '\n' -v localhost:10000/v2/data/datasets/profiles -XDELETE


Starting the Service and the Flow
---------------------------------

Once the application is deployed:

- Click on ``UserProfiles`` in the Overview page of the CDAP Console to get to the
  Application detail page, click:
  
  - ``ActivityFlow`` in the *Flow* pane to get to the Flow detail page, then click the *Start* button; or
  - ``UserProfileService`` in the *Service* pane to get to the Service detail page, then click the *Start* button; and
  
- From the Standalone CDAP SDK directory, use the Command Line Interface:

      - ``$ ./bin/cdap-cli.sh start flow UserProfiles.ActivityFlow``
      - ``$ ./bin/cdap-cli.sh start service UserProfiles.UserProfileService``

..  .. list-table::
..    :widths: 20 80
..    :stub-columns: 1

..    * - On Linux:
..      - ``$ ./bin/cdap-cli.sh start flow UserProfiles.ActivityFlow``
..    * - 
..      - ``$ ./bin/cdap-cli.sh start service UserProfiles.UserProfileService``
      
..    * - On Windows:
..      - ``> bin\cdap-cli.bat start flow UserProfiles.ActivityFlow``    
..    * - 
..      - ``> bin\cdap-cli.bat start service UserProfiles.UserProfileService``    

Populate the ``profiles`` Table
-------------------------------

Populate the ``profiles`` tables with users using a script. From the example's directory, use:

  - ``$ ./bin/add-users.sh``

..  .. list-table::
..    :widths: 20 80
..    :stub-columns: 1

..    * - On Linux:
..      - ``$ ./bin/add-users.sh``

..    * - On Windows:
..      - ``> bin\add-users.bat``    


Create a Conflict
-----------------
Now, from two
different terminals, run the following commands concurrently:

- ``bin/update-login.sh`` to randomly update the time of last login for users; and
- ``bin/send-events.sh`` to generate random user activity events and send them to the stream.

If both scripts are running at the same time, then some user profiles will be updated at
the same time by the Service and by the Flow. With row-level conflict detection, you would
see transaction conflicts in the logs. But when the ``profiles`` table uses
column-level conflict detection, these conflicts are avoided.

To see the behavior with row-level conflict detection, set
the dataset creation statement at the bottom of ``UserProfiles.java`` to use ``ConflictDetection.ROW``
and run the steps as above. You should see transaction conflicts in the logs. For example, such
a conflict would show as (reformatted to fit)::

  2015-01-26 21:00:20,084 - ERROR 
  [FlowletProcessDriver-updater-0-executor:c.c.c.i.a.r.f.FlowletProcessDriver@279] 
   - Transaction operation failed: Conflict detected for transaction 1422334820080000000.
  co.cask.tephra.TransactionConflictException: Conflict detected for transaction 1422334820080000000.
    at co.cask.tephra.TransactionContext.checkForConflicts(TransactionContext.java:166) ~[tephra-core-0.3.4.jar:na]
    at co.cask.tephra.TransactionContext.finish(TransactionContext.java:78) ~[tephra-core-0.3.4.jar:na]

Note that in order to see this happen (and to change from row- to column- and vice-versa),
you need to delete the ``profiles`` dataset before redeploying the application, to force
its recreation with the new properties.

Running the example with ``ConflictDetection.COLUMN`` will result in the two scripts running
concurrently without transaction conflicts.

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the step:

**Stopping the Service**

- Click on ``UserProfiles`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``ActivityFlow`` in the *Flow* section of the *Process* pane
  to get to the Flow's detail page, and then click the *Stop* button; repeat these steps for
  the ``FileSetService`` in the *Service* pane; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

      - ``$ ./bin/cdap-cli.sh stop flow UserProfiles.ActivityFlow``
      - ``$ ./bin/cdap-cli.sh stop service UserProfiles.UserProfileService``

..  .. list-table::
..    :widths: 20 80
..    :stub-columns: 1

..    * - On Linux:
..      - ``$ ./bin/cdap-cli.sh stop flow UserProfiles.ActivityFlow``
..    * - 
..      - ``$ ./bin/cdap-cli.sh stop service UserProfiles.UserProfileService``

..    * - On Windows:
..      - ``> bin\cdap-cli.bat stop flow UserProfiles.ActivityFlow``    
..    * - 
..      - ``> bin\cdap-cli.bat stop service UserProfiles.UserProfileService``    

