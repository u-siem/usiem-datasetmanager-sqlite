use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use rusqlite::{params, Connection};
use usiem::components::dataset::holder::DatasetHolder;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::sync::atomic::AtomicPtr;
use std::sync::{Arc, Mutex};
use usiem::components::common::SiemMessage;
use usiem::components::dataset::geo_ip::{GeoIpDataset, GeoIpInfo, GeoIpSynDataset, UpdateGeoIp};
use usiem::components::dataset::ip_map::{IpMapDataset, IpMapSynDataset, UpdateIpMap};
use usiem::components::dataset::ip_map_list::{UpdateIpMapList, IpMapListSynDataset, IpMapListDataset};
use usiem::components::dataset::ip_net::{IpNetDataset, IpNetSynDataset, UpdateNetIp};
use usiem::components::dataset::ip_set::{IpSetDataset, IpSetSynDataset, UpdateIpSet};
use usiem::components::dataset::text_map::{TextMapDataset, TextMapSynDataset, UpdateTextMap};
use usiem::components::dataset::text_map_list::{
    TextMapListDataset, TextMapListSynDataset, UpdateTextMapList,
};
use usiem::components::dataset::text_set::{TextSetDataset, TextSetSynDataset, UpdateTextSet};
use usiem::components::dataset::{SiemDataset, SiemDatasetType};
use usiem::components::SiemDatasetManager;
use usiem::events::field::SiemIp;

#[derive(Debug)]
struct KeyValTextMap {
    key: String,
    val: String,
}

enum UpdateListener {
    UpdateTextSet(Sender<UpdateTextSet>, Receiver<UpdateTextSet>, i64),
    UpdateTextMap(Sender<UpdateTextMap>, Receiver<UpdateTextMap>, i64),
    UpdateTextMapList(Sender<UpdateTextMapList>, Receiver<UpdateTextMapList>, i64),
    UpdateIpSet(Sender<UpdateIpSet>, Receiver<UpdateIpSet>, i64),
    UpdateNetIp(Sender<UpdateNetIp>, Receiver<UpdateNetIp>, i64),
    UpdateIpMapList(Sender<UpdateIpMapList>, Receiver<UpdateIpMapList>, i64),
    UpdateIpMap(Sender<UpdateIpMap>, Receiver<UpdateIpMap>, i64),
    UpdateGeoIp(Sender<UpdateGeoIp>, Receiver<UpdateGeoIp>, i64),
}

lazy_static! {
    static ref DATASETS: Arc<Mutex<BTreeMap<SiemDatasetType, SiemDataset>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
}

pub struct SqliteDatasetManager {
    /// Send actions to the kernel
    kernel_sender: Sender<SiemMessage>,
    /// Receive actions from other components or the kernel
    local_chnl_rcv: Receiver<SiemMessage>,
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    registered_datasets: BTreeMap<SiemDatasetType, UpdateListener>,
    conn: Connection,
    dataset_pointers : BTreeMap<SiemDatasetType, Arc<AtomicPtr<SiemDataset>>>,
    datasets : BTreeMap<SiemDatasetType, SiemDataset>,
    dataset_holder : DatasetHolder
}
impl SqliteDatasetManager {
    pub fn new(path: String) -> Result<SqliteDatasetManager, String> {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, local_chnl_rcv) = crossbeam_channel::unbounded();
        let conn = match Connection::open(&path) {
            Ok(conn) => conn,
            Err(e) => return Err(format!("{}", e)),
        };
        return Ok(SqliteDatasetManager {
            kernel_sender,
            local_chnl_rcv,
            local_chnl_snd,
            registered_datasets: BTreeMap::new(),
            conn,
            dataset_pointers : BTreeMap::new(),
            datasets : BTreeMap::new(),
            dataset_holder : DatasetHolder::from_datasets(vec![])
        });
    }

    pub fn debug() -> Result<SqliteDatasetManager, String> {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, local_chnl_rcv) = crossbeam_channel::unbounded();
        let conn = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(_) => return Err(String::from("")),
        };
        return Ok(SqliteDatasetManager {
            kernel_sender,
            local_chnl_rcv,
            local_chnl_snd,
            registered_datasets: BTreeMap::new(),
            conn,
            dataset_pointers : BTreeMap::new(),
            datasets : BTreeMap::new(),
            dataset_holder : DatasetHolder::from_datasets(vec![])
        });
    }
    fn create_text_map(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key TEXT NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);", dataset_name = name), []);
    }

    fn create_map_text_list(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key TEXT NOT NULL UNIQUE);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);CREATE TABLE IF NOT EXISTS dataset_list_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key INTEGER NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_list_{dataset_name}_data_key ON dataset_list_{dataset_name} (data_key);", dataset_name = name), []);
    }
    fn create_map_ip_net(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, network INTEGER NOT NULL, data_key BLOB NOT NULL, data_val TEXT NOT NULL); CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (network, data_key);", dataset_name = name), []);
    }

    fn create_geo_ip_net(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, network INTEGER NOT NULL, data_key BLOB NOT NULL, country TEXT NOT NULL, city TEXT NOT NULL, latitude TEXT NOT NULL, longitude TEXT NOT NULL, isp TEXT NOT NULL); CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (network, data_key);", dataset_name = name), []);
    }
    fn update_geo_ip(&self, name: &str, update: UpdateGeoIp) -> rusqlite::Result<()> {
        match update {
            UpdateGeoIp::Add((ip, net, info)) => {
                self.conn.execute(
                    &format!(
                        "INSERT INTO dataset_{dataset_name} (data_key, network, country, city, latitude, longitude, isp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip), net, info.country, info.city, info.latitude, info.longitude, info.isp],
                )?;
            }
            UpdateGeoIp::Remove((ip, net)) => {
                self.conn.execute(
                    &format!(
                        "DELETE FROM dataset_{dataset_name} WHERE data_key = ?1 AND network = ?2 LIMIT 1",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip), net],
                )?;
            }
            UpdateGeoIp::Replace(_dataset) => {
                self.conn.execute(
                    &format!("DELETE FROM dataset_{dataset_name} ", dataset_name = name),
                    [],
                )?;
                // TODO...
            }
        }
        return Ok(());
    }

    fn create_map_ip_list(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key BLOB NOT NULL UNIQUE);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);CREATE TABLE IF NOT EXISTS dataset_list_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key INTEGER NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_list_{dataset_name}_data_key ON dataset_list_{dataset_name} (data_key);", dataset_name = name), []);
    }
    fn update_map_ip_list(&self, name: &str, update: UpdateIpMapList) -> rusqlite::Result<()> {
        match update {
            UpdateIpMapList::Add((ip, txt)) => {
                self.conn.execute(
                    &format!(
                        "INSERT INTO dataset_{dataset_name} (data_key) VALUES (?1);",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip)],
                )?;
                let id = self.conn.last_insert_rowid();
                for el in txt {
                    self.conn.execute(
                        &format!(
                            "INSERT INTO dataset_list_{dataset_name} (data_key, data_val) VALUES (?1, ?2)",
                            dataset_name = name
                        ),
                        params![id, el],
                    )?;
                }
            }
            UpdateIpMapList::Remove(ip) => {
                self.conn.execute(
                    &format!(
                        "DELETE FROM dataset_{dataset_name} WHERE data_key = ?1 LIMIT 1;DELETE FROM dataset_list_{dataset_name} WHERE data_key = ?1;",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip)],
                )?;
            }
            UpdateIpMapList::Replace(_dataset) => {
                self.conn.execute(
                    &format!("DELETE FROM dataset_{dataset_name} ", dataset_name = name),
                    [],
                )?;
                // TODO...
            }
        }
        return Ok(());
    }

    fn create_ip_map(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key BLOB NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);", dataset_name = name), []);
    }
    fn update_map_ip(&self, name: &str, update: UpdateIpMap) -> rusqlite::Result<()> {
        match update {
            UpdateIpMap::Add((ip, txt)) => {
                self.conn.execute(
                    &format!(
                        "INSERT INTO dataset_{dataset_name} (data_key, data_val) VALUES (?1, ?2)",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip), txt],
                )?;
            }
            UpdateIpMap::Remove(ip) => {
                self.conn.execute(
                    &format!(
                        "DELETE FROM dataset_{dataset_name} WHERE data_key = ?1 LIMIT 1",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip)],
                )?;
            }
            UpdateIpMap::Replace(_dataset) => {
                self.conn.execute(
                    &format!("DELETE FROM dataset_{dataset_name} ", dataset_name = name),
                    [],
                )?;
            }
        }
        return Ok(());
    }

    fn create_ip_set(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key BLOB NOT NULL UNIQUE);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);", dataset_name = name), []);
    }

    fn update_ip_set(&self, name: &str, update: UpdateIpSet) -> rusqlite::Result<()> {
        match update {
            UpdateIpSet::Add(ip) => {
                self.conn.execute(
                    &format!(
                        "INSERT INTO dataset_{dataset_name} (data_key) VALUES (?1)",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip)],
                )?;
            }
            UpdateIpSet::Remove(ip) => {
                self.conn.execute(
                    &format!(
                        "DELETE FROM dataset_{dataset_name} WHERE data_key = ?1 LIMIT 1",
                        dataset_name = name
                    ),
                    params![ip_to_vec8(&ip)],
                )?;
            }
            UpdateIpSet::Replace(dataset) => {
                self.conn.execute(
                    &format!("DELETE FROM dataset_{dataset_name} ", dataset_name = name),
                    [],
                )?;
                let (ip4, ip6) = dataset.internal_ref();
                for ip in ip4 {
                    self.conn.execute(
                        &format!(
                            "INSERT INTO dataset_{dataset_name} (data_key) VALUES (?1)",
                            dataset_name = name
                        ),
                        params![ip.to_le_bytes().to_vec()],
                    )?;
                }
                for ip in ip6 {
                    self.conn.execute(
                        &format!(
                            "INSERT INTO dataset_{dataset_name} (data_key) VALUES (?1)",
                            dataset_name = name
                        ),
                        params![ip.to_le_bytes().to_vec()],
                    )?;
                }
            }
        }
        return Ok(());
    }

    fn create_text_list(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, network INTEGER NOT NULL, data_key BLOB NOT NULL, data_val TEXT NOT NULL); CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (network, data_key);", dataset_name = name), []);
    }
}

impl SiemDatasetManager for SqliteDatasetManager {
    fn name(&self) -> &str {
        &"SqliteDatasetManager"
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn set_kernel_sender(&mut self, sender: Sender<SiemMessage>) {
        self.kernel_sender = sender;
    }

    fn run(&mut self) {
        loop {
            match self.local_chnl_rcv.try_recv() {
                Ok(_msg) => {}
                Err(e) => match e {
                    crossbeam_channel::TryRecvError::Empty => {}
                    crossbeam_channel::TryRecvError::Disconnected => {
                        panic!("DatasetManager channel disconected!!")
                    }
                },
            }
            let mut updated_datasets = BTreeSet::new();
            let time = chrono::Utc::now().timestamp_millis();
            for (dataset_name, listener) in self.registered_datasets.iter() {
                match listener {
                    UpdateListener::UpdateIpMap(_s, r, t) => {
                        if (*t + 5000) < time {
                            loop {
                                match r.try_recv() {
                                    Ok(update) => {
                                        let name = format!("{:?}", dataset_name);
                                        let _ = self.update_map_ip(&name[..], update);
                                        updated_datasets.insert(dataset_name.clone());
                                    }
                                    Err(e) => match e {
                                        crossbeam_channel::TryRecvError::Empty => {
                                            break;
                                        }
                                        crossbeam_channel::TryRecvError::Disconnected => {
                                            panic!("DatasetManager channel disconected!!")
                                        }
                                    },
                                }
                            }
                        }
                    }
                    UpdateListener::UpdateIpSet(_s, r, t) => {
                        if (*t + 5000) < time {
                            loop {
                                match r.try_recv() {
                                    Ok(update) => {
                                        let name = format!("{:?}", dataset_name);
                                        let _ = self.update_ip_set(&name[..], update);
                                        updated_datasets.insert(dataset_name.clone());
                                    }
                                    Err(e) => match e {
                                        crossbeam_channel::TryRecvError::Empty => {
                                            break;
                                        }
                                        crossbeam_channel::TryRecvError::Disconnected => {
                                            panic!("DatasetManager channel disconected!!")
                                        }
                                    },
                                }
                            }
                        }
                    }
                    UpdateListener::UpdateIpMapList(_s, r, t) => {
                        if (*t + 5000) < time {
                            loop {
                                match r.try_recv() {
                                    Ok(update) => {
                                        let name = format!("{:?}", dataset_name);
                                        let _ = self.update_map_ip_list(&name[..], update);
                                        updated_datasets.insert(dataset_name.clone());
                                    }
                                    Err(e) => match e {
                                        crossbeam_channel::TryRecvError::Empty => {
                                            break;
                                        }
                                        crossbeam_channel::TryRecvError::Disconnected => {
                                            panic!("DatasetManager channel disconected!!")
                                        }
                                    },
                                }
                            }
                        }
                    }
                    UpdateListener::UpdateGeoIp(_s, r, t) => {
                        if (*t + 5000) < time {
                            loop {
                                match r.try_recv() {
                                    Ok(update) => {
                                        let name = format!("{:?}", dataset_name);
                                        let _ = self.update_geo_ip(&name[..], update);
                                        updated_datasets.insert(dataset_name.clone());
                                    }
                                    Err(e) => match e {
                                        crossbeam_channel::TryRecvError::Empty => {
                                            break;
                                        }
                                        crossbeam_channel::TryRecvError::Disconnected => {
                                            panic!("DatasetManager channel disconected!!")
                                        }
                                    },
                                }
                            }
                        }
                    }
                    // TODO
                    _ => {}
                }
            }
            let mut new_datasets = Vec::new();

            for data_name in &updated_datasets {
                match self.registered_datasets.get_mut(data_name) {
                    Some(v) => match v {
                        //TODO: Add more cases
                        UpdateListener::UpdateIpMap(s, _, t) => {
                            *t = time;
                            let new_dataset =
                                match dataset_ip_map(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update MapIp dataset")
                                    }
                                };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                IpMapSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        UpdateListener::UpdateIpSet(s, _, t) => {
                            *t = time;
                            let new_dataset =
                                match dataset_ip_set(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update IpSet dataset")
                                    }
                                };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                IpSetSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        UpdateListener::UpdateTextMap(s, _, t) => {
                            *t = time;
                            let new_dataset =
                                match dataset_text_map(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update TextMap dataset")
                                    }
                                };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                TextMapSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        UpdateListener::UpdateTextMapList(s, _, t) => {
                            *t = time;
                            let new_dataset = match dataset_map_text_list(
                                &self.conn,
                                &format!("{:?}", data_name),
                            ) {
                                Ok(d) => d,
                                Err(_) => {
                                    panic!("Cannot update TextMapList dataset")
                                }
                            };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                TextMapListSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        UpdateListener::UpdateTextSet(s, _, t) => {
                            *t = time;
                            let new_dataset =
                                match dataset_text_list(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update TextSet dataset")
                                    }
                                };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                TextSetSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        UpdateListener::UpdateGeoIp(s, _, t) => {
                            *t = time;
                            let new_dataset =
                                match dataset_geo_ip_net(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update GeoIp dataset")
                                    }
                                };
                            match SiemDataset::try_from((
                                data_name.clone(),
                                GeoIpSynDataset::new(Arc::from(new_dataset), s.clone()),
                            )) {
                                Ok(nw) => {
                                    new_datasets.push(nw);
                                }
                                Err(_) => {}
                            }
                        }
                        _ => {}
                    },
                    None => {}
                }
            }

            // Update last build time and also Build the references
            loop {
                if new_datasets.is_empty() {
                    break;
                } else {
                    let dataset = new_datasets.remove(0);
                    let typ = dataset.dataset_type();
                    let dataset_pointer = match self.dataset_pointers.get(&typ) {
                        Some(dt) => dt,
                        None => {
                            panic!("Dataset not found!?!?");
                        }
                    };
                    self.datasets.insert(dataset.dataset_type(), dataset);
                    let dataset_ref = match self.datasets.get_mut(&typ) {
                        Some(dt) => dt,
                        None => {
                            panic!("Dataset not found!?!?");
                        }
                    };
                    dataset_pointer.store(dataset_ref, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    fn get_datasets(&self) -> DatasetHolder {
        self.dataset_holder.clone()
    }
    fn register_dataset(&mut self, dataset_type: SiemDatasetType) {
        let time = chrono::Utc::now().timestamp_millis();
        if !self.registered_datasets.contains_key(&dataset_type) {
            let (listener, dataset): (UpdateListener, SiemDataset) = match &dataset_type {
                SiemDatasetType::CustomMapText(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map(&name);
                    let dataset = match dataset_text_map(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: UserTag")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::CustomMapText((name.clone(),syn_dataset)))
                }
                SiemDatasetType::CustomIpList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_ip_set(&name);
                    let dataset = match dataset_ip_set(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: UserTag")
                    };
                    let syn_dataset = IpSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateIpSet(channel.0, channel.1, time), SiemDataset::CustomIpList((name.clone(),syn_dataset)))

                }
                SiemDatasetType::CustomMapIpNet(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_ip_net(&name);
                    let dataset = match dataset_ip_net(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: CustomMapIpNet")
                    };
                    let syn_dataset = IpNetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateNetIp(channel.0, channel.1, time), SiemDataset::CustomMapIpNet((name.clone(),syn_dataset)))
                }
                SiemDatasetType::CustomIpMap(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_ip_map(&name);
                    let dataset = match dataset_ip_map(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: CustomIpMap")
                    };
                    let syn_dataset = IpMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateIpMap(channel.0, channel.1, time), SiemDataset::CustomIpMap((name.clone(),syn_dataset)))
                }
                SiemDatasetType::CustomMapTextList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_text_list(&name);
                    let dataset = match dataset_map_text_list(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: CustomMapTextList")
                    };
                    let syn_dataset = TextMapListSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMapList(channel.0, channel.1, time), SiemDataset::CustomMapTextList((name.clone(),syn_dataset)))
                }
                SiemDatasetType::CustomTextList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_list(&name);
                    let dataset = match dataset_text_list(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: CustomTextList")
                    };
                    let syn_dataset = TextSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextSet(channel.0, channel.1, time), SiemDataset::CustomTextList((name.clone(),syn_dataset)))
                }
                SiemDatasetType::Secrets(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map(&name);
                    let dataset = match dataset_text_map(&self.conn, &name) {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: Secrets")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::Secrets((name.clone(),syn_dataset)))
                }
                SiemDatasetType::GeoIp => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_geo_ip_net("GeoIp");
                    let dataset = match dataset_geo_ip_net(&self.conn,"GeoIp") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: GeoIp")
                    };
                    let syn_dataset = GeoIpSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateGeoIp(channel.0, channel.1, time), SiemDataset::GeoIp(syn_dataset))
                }
                SiemDatasetType::IpMac => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_ip_map("IpMac");
                    let dataset = match dataset_ip_map(&self.conn,"IpMac") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: IpMac")
                    };
                    let syn_dataset = IpMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateIpMap(channel.0, channel.1, time), SiemDataset::IpMac(syn_dataset))
                }
                SiemDatasetType::IpDNS => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_ip_list("IpDNS");
                    let dataset = match dataset_ip_map_list(&self.conn,"IpDNS") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: IpDNS")
                    };
                    let syn_dataset = IpMapListSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateIpMapList(channel.0, channel.1, time), SiemDataset::IpDNS(syn_dataset))
                }
                SiemDatasetType::MacHost => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map("MacHost");
                    let dataset = match dataset_text_map(&self.conn,"MacHost") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: MacHost")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::MacHost(syn_dataset))
                }
                SiemDatasetType::HostUser => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map("HostUser");
                    let dataset = match dataset_text_map(&self.conn,"HostUser") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: HostUser")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::HostUser(syn_dataset))
                }
                SiemDatasetType::BlockIp => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_ip_set("BlockIp");
                    let dataset = match dataset_ip_set(&self.conn,"BlockIp") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: BlockIp")
                    };
                    let syn_dataset = IpSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateIpSet(channel.0, channel.1, time), SiemDataset::BlockIp(syn_dataset))
                }
                SiemDatasetType::BlockDomain => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_list("BlockDomain");
                    let dataset = match dataset_text_list(&self.conn,"BlockDomain") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: BlockDomain")
                    };
                    let syn_dataset = TextSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextSet(channel.0, channel.1, time), SiemDataset::BlockDomain(syn_dataset))
                }
                SiemDatasetType::BlockEmailSender => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_list("BlockEmailSender");
                    let dataset = match dataset_text_list(&self.conn,"BlockEmailSender") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: BlockEmailSender")
                    };
                    let syn_dataset = TextSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextSet(channel.0, channel.1, time), SiemDataset::BlockEmailSender(syn_dataset))
                }
                SiemDatasetType::BlockCountry => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_list("BlockCountry");
                    let dataset = match dataset_text_list(&self.conn,"BlockCountry") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: BlockCountry")
                    };
                    let syn_dataset = TextSetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextSet(channel.0, channel.1, time), SiemDataset::BlockCountry(syn_dataset))
                }
                SiemDatasetType::HostVulnerable => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_ip_map("HostVulnerable");
                    let dataset = match dataset_map_text_list(&self.conn,"HostVulnerable") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: HostVulnerable")
                    };
                    let syn_dataset = TextMapListSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMapList(channel.0, channel.1, time), SiemDataset::HostVulnerable(syn_dataset))
                }
                SiemDatasetType::UserTag => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_text_list("UserTag");
                    let dataset = match dataset_map_text_list(&self.conn, "UserTag") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: UserTag")
                    };
                    let syn_dataset = TextMapListSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMapList(channel.0, channel.1, time), SiemDataset::UserTag(syn_dataset))
                }
                SiemDatasetType::AssetTag => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_text_list("AssetTag");
                    let dataset = match dataset_map_text_list(&self.conn, "AssetTag") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: AssetTag")
                    };
                    let syn_dataset = TextMapListSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMapList(channel.0, channel.1, time), SiemDataset::AssetTag(syn_dataset))
                }
                SiemDatasetType::IpCloudService => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_ip_net("IpCloudService");
                    let dataset = match dataset_ip_net(&self.conn, "IpCloudService") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: IpCloudService")
                    };
                    let syn_dataset = IpNetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateNetIp(channel.0, channel.1, time), SiemDataset::IpCloudService(syn_dataset))
                }
                SiemDatasetType::IpCloudProvider => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_ip_net("IpCloudProvider");
                    let dataset = match dataset_ip_net(&self.conn, "IpCloudProvider") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: IpCloudProvider")
                    };
                    let syn_dataset = IpNetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateNetIp(channel.0, channel.1, time), SiemDataset::IpCloudProvider(syn_dataset))
                }
                SiemDatasetType::UserHeadquarters => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map("UserHeadquarters");
                    let dataset = match dataset_text_map(&self.conn, "UserHeadquarters") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: UserHeadquarters")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::UserHeadquarters(syn_dataset))
                }
                SiemDatasetType::IpHeadquarters => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_map_ip_net("IpHeadquarters");
                    let dataset = match dataset_ip_net(&self.conn, "IpHeadquarters") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: IpHeadquarters")
                    };
                    let syn_dataset = IpNetSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateNetIp(channel.0, channel.1, time), SiemDataset::IpHeadquarters(syn_dataset))
                }
                SiemDatasetType::Configuration => {
                    let channel = crossbeam_channel::bounded(128);
                    self.create_text_map("Configuration");
                    let dataset = match dataset_text_map(&self.conn, "Configuration") {
                        Ok(d) => d,
                        Err(_) => panic!("Cannot init dataset: Configuration")
                    };
                    let syn_dataset = TextMapSynDataset::new(Arc::new(dataset),channel.0.clone());
                    (UpdateListener::UpdateTextMap(channel.0, channel.1, time), SiemDataset::Configuration(syn_dataset))
                }
                _ => {
                    println!("Dataset type not defined!!!");
                    return;
                }
            };
            self.registered_datasets.insert(dataset_type.clone(), listener);
            self.datasets.insert(dataset_type.clone(), dataset);
            match self.datasets.get_mut(&dataset_type) {
                Some(v) => {
                    let pntr = Arc::new(AtomicPtr::new(v));
                    self.dataset_pointers.insert(dataset_type.clone(), pntr);
                },
                None => {
                    panic!("Cannot found dataset!!!");
                }
            };
            let mut pointer_list = Vec::with_capacity(self.dataset_pointers.len());
            for (_typ, pntr) in &self.dataset_pointers {
                pointer_list.push(pntr.clone());
            }
            self.dataset_holder = DatasetHolder::from_datasets(pointer_list);
        }
    }
}

fn ip_form_vec8(v: &Vec<u8>) -> Result<SiemIp, ()> {
    if v.len() == 4 {
        return Ok(SiemIp::V4(
            ((v[3] as u32) << 24) | ((v[2] as u32) << 16) | ((v[1] as u32) << 8) | (v[0] as u32),
        ));
    } else if v.len() == 16 {
        return Ok(SiemIp::V6(
            ((v[15] as u128) << 120)
                | ((v[14] as u128) << 112)
                | ((v[13] as u128) << 104)
                | ((v[12] as u128) << 96)
                | ((v[11] as u128) << 88)
                | ((v[10] as u128) << 80)
                | ((v[9] as u128) << 72)
                | ((v[8] as u128) << 64)
                | ((v[7] as u128) << 56)
                | ((v[6] as u128) << 48)
                | ((v[5] as u128) << 40)
                | ((v[4] as u128) << 32)
                | ((v[3] as u128) << 24)
                | ((v[2] as u128) << 16)
                | ((v[1] as u128) << 8)
                | (v[0] as u128),
        ));
    } else {
        return Err(());
    }
}

fn ip_to_vec8(ip: &SiemIp) -> Vec<u8> {
    match ip {
        SiemIp::V4(v4) => v4.to_le_bytes().to_vec(),
        SiemIp::V6(v6) => v6.to_le_bytes().to_vec(),
    }
}

fn dataset_ip_set(conn: &Connection, name: &str) -> rusqlite::Result<IpSetDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT data_key FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok(row.get(0)?))?;
    let mut dataset = IpSetDataset::new();
    for row in iterator {
        let k_v: String = row?;
        match SiemIp::from_ip_str(&k_v) {
            Ok(ip) => dataset.insert(ip),
            Err(_) => return Err(rusqlite::Error::SqliteSingleThreadedMode),
        }
    }
    return Ok(dataset);
}
fn dataset_text_list(conn: &Connection, name: &str) -> rusqlite::Result<TextSetDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT data_key FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok(row.get(0)?))?;
    let mut dataset = TextSetDataset::new();
    for row in iterator {
        let k_v = row?;
        dataset.insert(Cow::Owned(k_v))
    }
    return Ok(dataset);
}
fn dataset_ip_map(conn: &Connection, name: &str) -> rusqlite::Result<IpMapDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT data_key, data_val FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut dataset = IpMapDataset::new();
    for row in iterator {
        let (k, v): (Vec<u8>, String) = row?;
        match ip_form_vec8(&k) {
            Ok(ip) => dataset.insert(ip, Cow::Owned(v)),
            Err(_) => return Err(rusqlite::Error::SqliteSingleThreadedMode),
        }
    }
    return Ok(dataset);
}
fn dataset_ip_map_list(conn: &Connection, name: &str) -> rusqlite::Result<IpMapListDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT data_key, data_val FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut dataset = IpMapListDataset::new();
    for row in iterator {
        let (k, v): (Vec<u8>, String) = row?;
        match ip_form_vec8(&k) {
            Ok(ip) => dataset.insert(ip, v.split("|").map(|v| Cow::Owned(v.to_string())).collect()),
            Err(_) => return Err(rusqlite::Error::SqliteSingleThreadedMode),
        }
    }
    return Ok(dataset);
}
fn dataset_ip_net(conn: &Connection, name: &str) -> rusqlite::Result<IpNetDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT network, data_key, data_val FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    let mut dataset = IpNetDataset::new();
    for row in iterator {
        let (net, ip, val): (u8, Vec<u8>, String) = row?;
        match ip_form_vec8(&ip) {
            Ok(ip) => dataset.insert(ip, net,Cow::Owned(val)),
            Err(_) => return Err(rusqlite::Error::SqliteSingleThreadedMode),
        }
    }
    return Ok(dataset);
}
fn dataset_text_map(conn: &Connection, name: &str) -> rusqlite::Result<TextMapDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT data_key, data_val FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| {
        Ok(KeyValTextMap {
            key: row.get(0)?,
            val: row.get(1)?,
        })
    })?;
    let mut dataset = TextMapDataset::new();
    for row in iterator {
        let k_v = row?;
        dataset.insert(Cow::Owned(k_v.key), Cow::Owned(k_v.val))
    }
    return Ok(dataset);
}
fn dataset_map_text_list(conn: &Connection, name: &str) -> rusqlite::Result<TextMapListDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT t1.data_key, t2.data_val FROM dataset_{dataset_name} as t1 INNER JOIN dataset_list_{dataset_name} as t2 ON t1.id = t2.data_key",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut dataset = TextMapListDataset::new();
    let mut bt: BTreeMap<String, Vec<Cow<'static, str>>> = BTreeMap::new();

    for row in iterator {
        let (k, v): (String, String) = row?;
        if bt.contains_key(&k) {
            match bt.get_mut(&k) {
                Some(ve) => ve.push(Cow::Owned(v)),
                None => {}
            }
        } else {
            bt.insert(k.to_string(), vec![Cow::Owned(v)]);
        }
    }
    for (k, v) in bt.into_iter() {
        dataset.insert(Cow::Owned(k), v);
    }
    return Ok(dataset);
}
fn dataset_geo_ip_net(conn: &Connection, name: &str) -> rusqlite::Result<GeoIpDataset> {
    let mut stmt = conn.prepare(&format!(
        "SELECT network, data_key, country, city, latitude, longitude, isp FROM dataset_{dataset_name}",
        dataset_name = name
    ))?;
    let iterator = stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        ))
    })?;
    let mut dataset = GeoIpDataset::new();

    for row in iterator {
        let (n, k, country, city, latitude, longitude, isp): (
            u8,
            Vec<u8>,
            String,
            String,
            f32,
            f32,
            String,
        ) = row?;
        match ip_form_vec8(&k) {
            Ok(k) => {
                dataset.insert(
                    k,
                    n,
                    GeoIpInfo {
                        country: Cow::Owned(country),
                        city: Cow::Owned(city),
                        latitude,
                        longitude,
                        isp: Cow::Owned(isp),
                    },
                );
            }
            Err(_) => {}
        }
    }
    return Ok(dataset);
}

#[cfg(test)]
mod tests {
    use usiem::{
        components::{
            command::{SiemCommandCall, SiemCommandHeader},
            common::{SiemComponentCapabilities, SiemComponentStateStorage},
            dataset::holder::DatasetHolder,
            SiemComponent,
        },
        events::SiemLog,
    };

    use super::*;

    #[derive(Clone)]
    pub struct BasicComponent {
        /// Send actions to the kernel
        kernel_sender: Sender<SiemMessage>,
        /// Send actions to this components
        local_chnl_snd: Sender<SiemMessage>,
        /// Receive logs
        log_receiver: Receiver<SiemLog>,
        log_sender: Sender<SiemLog>,
        datasets: DatasetHolder,
        id: u64,
    }
    impl BasicComponent {
        pub fn new() -> BasicComponent {
            let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
            let (local_chnl_snd, _local_chnl_rcv) = crossbeam_channel::unbounded();
            let (log_sender, log_receiver) = crossbeam_channel::unbounded();
            return BasicComponent {
                kernel_sender,
                local_chnl_snd,
                log_receiver,
                log_sender,
                id: 0,
                datasets: DatasetHolder::from_datasets(vec![]),
            };
        }
    }

    impl SiemComponent for BasicComponent {
        fn id(&self) -> u64 {
            return self.id;
        }
        fn set_id(&mut self, id: u64) {
            self.id = id;
        }
        fn name(&self) -> &str {
            "BasicParser"
        }
        fn local_channel(&self) -> Sender<SiemMessage> {
            self.local_chnl_snd.clone()
        }
        fn set_log_channel(&mut self, log_sender: Sender<SiemLog>, receiver: Receiver<SiemLog>) {
            self.log_receiver = receiver;
            self.log_sender = log_sender;
        }
        fn set_kernel_sender(&mut self, sender: Sender<SiemMessage>) {
            self.kernel_sender = sender;
        }
        fn duplicate(&self) -> Box<dyn SiemComponent> {
            return Box::new(self.clone());
        }
        fn set_datasets(&mut self, datasets: DatasetHolder) {
            self.datasets = datasets;
        }
        fn run(&mut self) {
            match self.datasets.get(&SiemDatasetType::IpMac) {
                Some(dataset) => match dataset {
                    SiemDataset::IpMac(dataset) => {
                        for i in 0..10000 {
                            dataset.insert(SiemIp::V4(i), Cow::Owned(format!("IP:{}", i)));
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }
        fn set_storage(&mut self, _conn: Box<dyn SiemComponentStateStorage>) {}

        /// Capabilities and actions that can be performed on this component
        fn capabilities(&self) -> SiemComponentCapabilities {
            SiemComponentCapabilities::new(
                Cow::Borrowed("BasicDummyComponent"),
                Cow::Borrowed("Basic dummy component for testing purposes"),
                Cow::Borrowed(""), // No HTML
                vec![],
                vec![],
                vec![],
                vec![],
            )
        }
    }

    #[test]
    fn test_ip_conversion() {
        let ip100 = SiemIp::V4(100);
        let arry = ip_to_vec8(&ip100);
        let ip100_2 = ip_form_vec8(&arry).unwrap();
        assert_eq!(ip100,ip100_2);

        let ip100 = SiemIp::V6(1234567);
        let arry = ip_to_vec8(&ip100);
        let ip100_2 = ip_form_vec8(&arry).unwrap();
        assert_eq!(ip100,ip100_2);
    }

    #[test]
    fn test_kernel_instance() {
        let mut comp = BasicComponent::new();

        let mut manager = match SqliteDatasetManager::debug() {
            Ok(manager) => manager,
            Err(_) => {
                panic!("Cannot initialize DatasetManager")
            }
        };
        manager.register_dataset(SiemDatasetType::IpMac);

        let dataset_list = manager.get_datasets();
        comp.set_datasets(dataset_list);

        let local_chan = manager.local_channel();
        let dataset_list = manager.get_datasets();
        std::thread::spawn(move || manager.run());
        std::thread::spawn(move || comp.run());

        std::thread::sleep(std::time::Duration::from_secs(7));
        
        
        match dataset_list.get(&SiemDatasetType::IpMac) {
            Some(val) => match val {
                SiemDataset::IpMac(ip_mac) => {
                    match ip_mac.get(&SiemIp::V4(100)) {
                        Some(_v) => {}
                        None => {
                            panic!("The component should update the dataset!");
                        }
                    }
                },
                _ => {
                    panic!("Dataset is not SiemDataset::IpMac")
                }
            },
            None => {
                panic!("No datasets availables")
            }
        }

        let _ = local_chan.send(SiemMessage::Command(
            SiemCommandHeader {
                user: String::from("None"),
                comp_id: 0,
                comm_id: 0,
            },
            SiemCommandCall::STOP_COMPONENT("Stop!!".to_string()),
        ));
    }
}
