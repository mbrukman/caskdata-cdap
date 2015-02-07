.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _schedules:

============================================
Schedules
============================================

**Schedules** are used to execute :ref:`Workflows <workflows>` on a recurring basis. You can add a Schedule (such as a
`crontab schedule <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(java.lang.String,%20java.lang.String,%20java.lang.String)>`__)
to the Workflow::

    scheduleWorkflow("FiveHourSchedule", "0 */5 * * *", "MyWorkflow");

The ``MyWorkflow`` will then be executed every 5 hours.

Optionally, you can specify the properties for the Schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow("FiveHourSchedule", "0 */5 * * *", "MyWorkflow", scheduleProperties);
    ...

Every time the ``FiveHourSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.
The properties specified by the Schedule can be overridden by the Workflow's runtime arguments, with the Workflow's having precedence.
