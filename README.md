Sql Dependency Provider for IIS and Azure
=========================================

Read more about this <a href="http://www.dotcastle.com/blog/sql-dependency-provider" target="_blank">here</a>

Features:
---------
1. Support for Azure web environment, which doesn't support enablement of Sql Service Broker for notifications 
2. Support for IIS/IIS Express hosted applications
3. Simple unified interface and seamless implementation fallbacks
4. Implements .NET SqlDependency framework based implementation on supported database servers 
5. Implements Polling model on database servers which do not support service broker enablement
6. No need to subscribe/unsubscribe as in .NET SqlDependency implementation
7. Customizable options 
8. Watch for multiple change types insert/update/delete
9. Watch for multiple tables using a single dependency provider
10. Uses simple database triggers to implement service broker and polling notifications

<hr />
Author: Ravi Kiran Katha<br />
Owner: DotCastle TechnoSolutions Private Limited<br />
Web: http://www.dotcastle.com<br />
Copyright: Copyright © 2013 - 2016, DotCastle TechnoSolutions Private Limited, INDIA. All rights reserved.

