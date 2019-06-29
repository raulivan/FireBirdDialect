using System.Collections.Generic;

namespace FireBirdDialect
{
    public static class DataCache
    {
        public static List<ItemCache> FieldsKeys = new List<ItemCache>();

        public static List<ItemCache> FieldsTable = new List<ItemCache>();
    }


    public struct ItemCache
    {
        public string Field { get; set; }
        public string Table { get; set; }
    }
}
