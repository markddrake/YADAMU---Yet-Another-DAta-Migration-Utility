# YADAMU Graphical User Interface.

The YADAMU Graphical User Interace is build using electron. 
The interface consists of two panels: The source panel and the target panel. The source panel is used to configure where YADAMU is to read data from. The target panel is used to configure where YADAMU is to write data to. Both panels consist of a series of tabs which allow the user to select a file or enter connnection infomration for a database. Once Source and Target information has been defined use the "Perform Task" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/cogs.png" width="20"> button to initiate the operation. Use the "Save Configuration" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/save.png" width="20"> button to create a YADAMU configuration file that can be used to repeat the current operation using CONFIG option of the Command Line Interface's copy command.

Note The "Perform Task" and "Save Configuration" buttons only become active once both connections have been tested successfully.


The following screenshot show the GUI ready to perform a copy of the HR schema from Oracle to postrgress. 

<img src="https://github.com/markddrake/YADAMU---Yet-Another-DAta-Migration-Utility/blob/master/md/assets/screenshots/YADAMU_GUI%231.JPG">

To start the GUI simply type bin\yadamu from the YADAMU directory. This should launch the application and dispay the GUI 


