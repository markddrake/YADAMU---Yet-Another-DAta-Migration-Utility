# YADAMU Graphical User Interface.

The YADAMU Graphical User Interace is built using electron. 
The interface consists of two panels; the source panel and the target panel. The source panel specfies where YADAMU is to read data from. The target panel specifies where YADAMU is to write data to. Both panels consist of a set of tabs that allow the user to select a file or enter connnection information for a database. Once Source and Target information has been defined the "Perform Task" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/cogs.png" width="20"> button is used to initiate the operation. The "Save Configuration" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/save.png" width="20"> button is used to create a YADAMU configuration file that can repeat the current operation via the CONFIG option of the Command Line Interface's copy command.

Note the "Perform Task" and "Save Configuration" buttons only become active once the source and target connections have been succesfully tested.


The following screenshot shows the GUI ready to perform a copy of the HR schema from Oracle to Postgres. 

<img src="assets/screenshots/YADAMU_GUI#.JPG">

To start the GUI simply type bin\yadamu from the YADAMU directory.

Clicking on the "Log Window" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/binoculars.png" width="20"> button will open the log window. The log window is used to mointor or review the results of the current operation.
