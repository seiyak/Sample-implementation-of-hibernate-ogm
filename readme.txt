This is an experimenting sample implementation of hibernate-ogm for NoSQL databases aiming at Riak,Redis and Voldemort currently. 
Once everything including my current implementation and hibernate ogm itself is correct, Query part will be implemented. Again,
this is still experimenting, so there might be something incorrect. I'll fix it shortly, since check and update the source code almost everyday.

How to query on NoSQL databases is different, so abstracting the differences will be a interesting
point. Will be implemented shortly.

To see how those databases are implemented, please take a look at org/hibernate/ogm/datastore/mapbased/impl package. You'll see
the corresponding datastore providers.

Hibernate OGM
==================================================
Version: 3.0.0.Alpha2

Description
-----------

Hibernate OGM is an attempt to store data in a NoSQL data grid using he Hibernate Core engine rather than rewriting a JPA engine from scratch.

The benefits are fairly obvious:
 - reimplementing the complex JPA specification is a lot of work
 - a new implementation would mature at a rather slow rate and risk of bugs would be high
 - Hibernate is familiar to many people

Instructions
------------

Checkout http://hibernate.org/subprojects/ogm for more information
The code is available on GitHub at https://github.com/hibernate/hibernate-ogm

Contact
------------

Latest Documentation:

   http://community.jboss.org/en/hibernate/ogm

Bug Reports:

   Hibernate JIRA (preferred): http://opensource.atlassian.com/projects/hibernate/browse/OGM
   hibernate-dev@lists.jboss.org

Free Technical Support:

   http://forum.hibernate.org (https://forum.hibernate.org/viewforum.php?f=31)

Notes
-----------

If you want to contribute, come to the hibernate-dev@lists.jboss.org mailing list
or join us on #hibernate-dev on freenode (login required)

This software and its documentation are distributed under the terms of the
FSF Lesser Gnu Public License (see license.txt).
