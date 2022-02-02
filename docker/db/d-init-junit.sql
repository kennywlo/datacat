

-- only for development
set sql_mode = '';

delete from DatasetVersion where Dataset = (select Dataset from VerDataset where DatasetName = 'testds1');
delete from VerDataset where DatasetName = 'testds1';
delete from DatasetGroup where Name = 'testgroup';
delete from DatasetLogicalFolder where Name = 'testfolder';
delete from DatasetLogicalFolder where Name = 'testpath';
delete from DatasetDataType where DatasetDataType = 'JUNIT_TEST';
delete from DatasetFileFormat where DataSetFileFormat = 'junit.test';

insert ignore into DatasetDataType (DatasetDataType) values ('JUNIT_TEST');
insert ignore into DatasetFileFormat (DatasetFileFormat) values ('junit.test');
insert ignore into DatasetSource (DatasetSource) values ('JUNIT_SOURCE');
insert ignore into DatasetSource (DatasetSource) values ('RESTFUL_API_v0.2');
insert ignore into DatasetSite (DatasetSite) values ('SLAC');

insert ignore into DatasetLogicalFolder (Name, Parent) values  ('testpath', (select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'ROOT'));
insert ignore into DatasetLogicalFolder (Name, Parent) values ('testfolder',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'testpath'));
insert ignore into DatasetGroup (Name, DatasetLogicalFolder) values ('testgroup',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'testpath'));
insert ignore into DatasetSite (DatasetSite) values ('OSN');


insert ignore into VerDataset (DatasetName, DataSetFileFormat, DataSetDataType, DatasetLogicalFolder, DatasetGroup)
        values ('testds1', 'junit.test', 'JUNIT_TEST', null, (select DatasetGroup from (select * from DatasetGroup) as x where Name = 'testgroup'));
              
insert ignore into DatasetVersion (Dataset, DataSetSource, ProcessInstance, TaskName)
        values ((select Dataset from VerDataset where DatasetName = 'testds1'), 'JUNIT_SOURCE', null, null);
        
insert ignore into DatasetVersion (Dataset, VersionID, DataSetSource, ProcessInstance, TaskName)
        values ((select Dataset from VerDataset where DatasetName = 'testds1'), 1, 'JUNIT_SOURCE', null, null);

insert ignore into DatasetVersion (Dataset, VersionID, DataSetSource, ProcessInstance, TaskName)
        values ((select Dataset from VerDataset where DatasetName = 'testds1'), 24, 'JUNIT_SOURCE', null, null);


insert ignore into DatasetLogicalFolder (Name, Parent) values ('a',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'testpath'));
insert ignore into DatasetLogicalFolder (Name, Parent) values ('b',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'a'));
insert ignore into DatasetLogicalFolder (name, Parent) values ('c',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'b'));

insert ignore into DatasetLogicalFolder (Name, Parent) values ('abc',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'testpath'));
insert ignore into DatasetLogicalFolder (Name, Parent) values ('def',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'abc'));
insert ignore into DatasetLogicalFolder (Name, Parent) values ('xyz',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'def'));

insert ignore into DatasetGroup (Name, DatasetLogicalFolder) values ('fed',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'abc'));
insert ignore into DatasetGroup (Name, DatasetLogicalFolder) values ('zyx',(select DatasetLogicalFolder from (select * from DatasetLogicalFolder) as x where Name = 'def'));

-- insert into DatasetMetaName (MetaName) VALUES ('nRun');
-- insert into DatasetMetaName (MetaName) VALUES ('alpha');
-- insert into DatasetMetaName (MetaName) VALUES ('num');
-- insert into DatasetMetaName (MetaName) VALUES ('sIntent');

insert ignore into DatasetMetaInfo (MetaName, ValueType) VALUES ('nRun', 'N');
insert ignore into DatasetMetaInfo (MetaName, ValueType) VALUES ('alpha', 'S');
insert ignore into DatasetMetaInfo (MetaName, ValueType) VALUES ('num', 'N');
insert ignore into DatasetMetaInfo (MetaName, ValueType) VALUES ('sIntent', 'S');

-- Initialize DatasetMetaInfo
--
-- insert into DatasetMetaInfo (MetaName, ValueType)
--    (
--    select AllNames.MetaName, AllNames.ValueType from
--        (select distinct MetaName MetaName, 'S' ValueType from VerDatasetMetaString
--           UNION
--         select distinct MetaName MetaName, 'N' ValueType from VerDatasetMetaNumber
--           UNION
--         select distinct MetaName MetaName, 'T' ValueType from VerDatasetMetaTimestamp) AllNames
--         LEFT OUTER JOIN DatasetMetaInfo CachedNames 
--           on (CachedNames.MetaName = AllNames.MetaName and CachedNames.ValueType = AllNames.ValueType)
--         WHERE CachedNames.MetaName is null
--     );
