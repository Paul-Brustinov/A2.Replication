using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceModel;

namespace A2.Replication.Data
{
    [ServiceContract]
    interface IReplicationService
    {
        [OperationContract]
        Int64 StartSession(Int64 ClientId);

        [OperationContract]
        void EndSession(Int64 ClientId, Int64 SessionId);

        [OperationContract]
        Int64 GetLastPackageId(Int64 ClientId, Int64 SessionId);

        [OperationContract]
        PackageData LoadPackage(Int64 ClientId, Int64 SessionId, Int64 PackageId);

        [OperationContract]
        void SendPackage(Int64 ClientId, Int64 SessionId, PackageData data);

        [OperationContract]
        void SendPackageContent(Int64 ClientId, Int64 SessionId, PackageData data);

        [OperationContract]
        DataForGet GetItemForGet(Int64 ClientId, Int64 SessionId);

        [OperationContract]
        PackageData GetDataForGet(Int64 ClientId, Int64 SessionId, DataForGet DataForGet);

        [OperationContract]
        void SetDataForGetSent(Int64 ClientId, Int64 SessionId, DataForGet DataForGet);
    }
}
