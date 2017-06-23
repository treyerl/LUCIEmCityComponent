using System;
using System.Drawing;
using Grasshopper.Kernel;

namespace LUCIEmCity
{
    public class LUCIEmCityInfo : GH_AssemblyInfo
    {
        public override string Name
        {
            get
            {
                return "LUCIEmCity";
            }
        }
        public override Bitmap Icon
        {
            get
            {
                //Return a 24x24 pixel bitmap to represent this GHA library.
                return null;
            }
        }
        public override string Description
        {
            get
            {
                //Return a short string describing the purpose of this GHA library.
                return "";
            }
        }
        public override Guid Id
        {
            get
            {
                return new Guid("c256932e-73c6-427f-b80f-6c340c56b3b9");
            }
        }

        public override string AuthorName
        {
            get
            {
                //Return a string identifying you or your company.
                return "";
            }
        }
        public override string AuthorContact
        {
            get
            {
                //Return a string representing your preferred contact details.
                return "";
            }
        }
    }
}
