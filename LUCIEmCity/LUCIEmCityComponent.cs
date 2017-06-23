using System;
using System.Collections.Generic;

using Grasshopper.Kernel;
using Rhino.Geometry;

using LuciCommunication;
using System.Text;

namespace LUCIEmCity
{
    public class LUCIEmCityComponent : GH_Component
    {
        /// <summary>
        /// Each implementation of GH_Component must provide a public 
        /// constructor without any arguments.
        /// Category represents the Tab in which the component will appear, 
        /// Subcategory the panel. If you use non-existing tab or panel names, 
        /// new tabs/panels will automatically be created.
        /// </summary>
        public LUCIEmCityComponent()
          : base("EmCity", "EmCity",
              "A component to connect EmCity from Grasshopper",
              "LUCI", "Communication")
        {
        }

        /// <summary>
        /// Registers all the input parameters for this component.
        /// </summary>
        protected override void RegisterInputParams(GH_Component.GH_InputParamManager pManager)
        {
            
            pManager.AddTextParameter("LUCI host address", "host", "The IP address of LUCI server which offers EmCity service", GH_ParamAccess.item, "localhost");
            pManager.AddIntegerParameter("LUCI host port", "port", "The port to which data has to be sent on LUCI server", GH_ParamAccess.item, 7654);
            pManager.AddTextParameter("Data", "Data", "Data to be sent to EmCity as a txt file in attachment", GH_ParamAccess.list);
        }

        ///// <summary>
        /// Registers all the output parameters for this component.
        /// </summary>
        protected override void RegisterOutputParams(GH_Component.GH_OutputParamManager pManager)
        {

        }

        /// <summary>
        /// This is the method that actually does the work.
        /// </summary>
        /// <param name="DA">The DA object can be used to retrieve data from input parameters and 
        /// to store data in output parameters.</param>
        protected override void SolveInstance(IGH_DataAccess DA)
        {
            string host = "";
            int port = 0;
            List<string> data = new List<string>();         

            if ((!DA.GetData<string>(0, ref host)))
                return;
            if ((!DA.GetData<int>(1, ref port)))
                return;
            if ((!DA.GetDataList<String>(2, data)))
                return;

            Communication2Luci c2l = new Communication2Luci();
            c2l.connect(host, port);
            c2l.setEnableThread(false);

            byte [] fileData = Encoding.UTF8.GetBytes(string.Join("\n", data));
            Communication2Luci.LuciStreamData streamData = new Communication2Luci.LuciStreamData() { Data = fileData, Format = "txt", Name = "typologies" };
            LuciCommunication.Communication2Luci.LuciAnswer answer = c2l.runService("EmCity", null, -1, streamData);
        }

        /// <summary>
        /// Provides an Icon for every component that will be visible in the User Interface.
        /// Icons need to be 24x24 pixels.
        /// </summary>
        protected override System.Drawing.Bitmap Icon
        {
            get
            {
                // You can add image files to your project resources and access them like this:
                //return Resources.IconForThisComponent;
                return null;
            }
        }

        /// <summary>
        /// Each component must have a unique Guid to identify it. 
        /// It is vital this Guid doesn't change otherwise old ghx files 
        /// that use the old ID will partially fail during loading.
        /// </summary>
        public override Guid ComponentGuid
        {
            get { return new Guid("{6ac1fb0c-4f4c-4218-9f9c-d3721680d92e}"); }
        }
    }
}
