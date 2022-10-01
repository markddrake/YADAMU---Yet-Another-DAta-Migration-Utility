# YADAMU Graphical User Interface.

The YADAMU Graphical User Interace is built using electron. 
The interface consists of two panels; the source panel and the target panel. The source panel specfies where YADAMU is to read data from. The target panel specifies where YADAMU is to write data to. Each panel contains a hamburger menu that allows the user to select the source and target for the copy operation. Once the source and target have been selected, enter the connection information and click "Test Connection" to check that information provided is valid. If the connection information is valid a 'Tick' will appear in the "Test Connection" button.

Note the "Perform Task" and "Save Configuration" buttons only become active once the source and target connections have been succesfully tested.


The following screenshot shows the GUI ready to perform a copy of the HR schema from Oracle to Postgres. 

![YADAMU GUI](assets/YADAMU_GUI%231.JPG)

To start the GUI simply type bin\yadamu from the YADAMU directory.

Clicking on the "Log Window" <img src="https://raw.githubusercontent.com/encharm/Font-Awesome-SVG-PNG/master/black/png/128/binoculars.png" width="20"> button will open the log window. The log window is used to mointor or review the results of the current operation.
