use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use rusqlite::{params, Connection};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};
use usiem::components::common::{
    SiemComponentCapabilities, SiemComponentStateStorage, SiemMessage,
};
use usiem::components::dataset::geo_ip::{GeoIpDataset, GeoIpInfo, GeoIpSynDataset, UpdateGeoIp};
use usiem::components::dataset::ip_map::{IpMapDataset, IpMapSynDataset, UpdateIpMap};
use usiem::components::dataset::ip_map_list::{
    IpMapListDataset, IpMapListSynDataset, UpdateIpMapList,
};
use usiem::components::dataset::ip_net::{IpNetDataset, IpNetSynDataset, UpdateNetIp};
use usiem::components::dataset::ip_set::{IpSetDataset, IpSetSynDataset, UpdateIpSet};
use usiem::components::dataset::text_map::{TextMapDataset, TextMapSynDataset, UpdateTextMap};
use usiem::components::dataset::text_map_list::{
    TextMapListDataset, TextMapListSynDataset, UpdateTextMapList,
};
use usiem::components::dataset::text_set::{TextSetDataset, TextSetSynDataset, UpdateTextSet};
use usiem::components::dataset::{SiemDataset, SiemDatasetType};
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::events::field::SiemIp;
use usiem::events::SiemLog;

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
    comp_channels: Arc<Mutex<BTreeMap<SiemDatasetType, Vec<Sender<SiemMessage>>>>>,
}
impl SqliteDatasetManager {
    pub fn new(path: String) -> Result<SqliteDatasetManager, String> {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, local_chnl_rcv) = crossbeam_channel::unbounded();
        let conn = match Connection::open(&path) {
            Ok(conn) => conn,
            Err(_) => return Err(String::from("")),
        };
        return Ok(SqliteDatasetManager {
            kernel_sender,
            local_chnl_rcv,
            local_chnl_snd,
            registered_datasets: BTreeMap::new(),
            conn,
            comp_channels: Arc::new(Mutex::new(BTreeMap::new())),
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
            comp_channels: Arc::new(Mutex::new(BTreeMap::new())),
        });
    }
    fn create_map_text(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key TEXT NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);", dataset_name = name), []);
    }

    fn create_map_text_list(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key TEXT NOT NULL UNIQUE);CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (data_key);CREATE TABLE IF NOT EXISTS dataset_list_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, data_key INTEGER NOT NULL UNIQUE, data_val TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS idx_list_{dataset_name}_data_key ON dataset_list_{dataset_name} (data_key);", dataset_name = name), []);
    }
    fn create_map_ip_net(&self, name: &str) {
        let _ = self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS dataset_{dataset_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, network INTEGER NOT NULL, data_key BLOB NOT NULL, data_val TEXT NOT NULL); CREATE UNIQUE INDEX IF NOT EXISTS idx_{dataset_name}_data_key ON dataset_{dataset_name} (network, data_key);", dataset_name = name), []);
    }
    fn dataset_map_ip_net(&self, name: &str) -> rusqlite::Result<IpNetDataset> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT network, data_key, data_val FROM dataset_{dataset_name}",
            dataset_name = name
        ))?;
        let iterator = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        let mut dataset = IpNetDataset::new();

        for row in iterator {
            let (n, k, v): (u8, Vec<u8>, String) = row?;
            match ip_form_vec8(&k) {
                Ok(k) => {
                    dataset.insert(k, n, Cow::Owned(v));
                }
                Err(_) => {}
            }
        }
        return Ok(dataset);
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

    fn create_map_ip(&self, name: &str) {
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
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("SiemDatasetManager")
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
                                match dataset_map_ip(&self.conn, &format!("{:?}", data_name)) {
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
                                match dataset_map_text(&self.conn, &format!("{:?}", data_name)) {
                                    Ok(d) => d,
                                    Err(_) => {
                                        panic!("Cannot update IpSet dataset")
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
                                    panic!("Cannot update IpSet dataset")
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
                                        panic!("Cannot update IpSet dataset")
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
                                        panic!("Cannot update IpSet dataset")
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
            match DATASETS.lock() {
                Ok(mut datasets) => {
                    match self.comp_channels.lock() {
                            Ok(comp_channels) => {
                                loop {
                                    if new_datasets.is_empty() {
                                        break;
                                    } else {
                                        let dataset = new_datasets.remove(0);
                                        datasets.insert(dataset.dataset_type(), dataset.clone());
                                        match comp_channels.get(&dataset.dataset_type()) {
                                            Some(comps) => {
                                                for comp_s in comps {
                                                    let _ = comp_s.send(SiemMessage::Dataset(dataset.clone()));
                                                }
                                            },
                                            None => {}
                                        }
                                    }
                                }
                            },
                            Err(_) => {}
                    }
                },
                Err(_) => {}
            }
        }
    }

    fn get_datasets(&self) -> Arc<Mutex<BTreeMap<SiemDatasetType, SiemDataset>>> {
        // Load datasets
        let data_mutex = Arc::clone(&DATASETS);
        match data_mutex.lock() {
            Ok(mut datasets) => {
                for (dataset_type, listener) in self.registered_datasets.iter() {
                    match dataset_type {
                        SiemDatasetType::CustomMapText(dataset_name) => {
                            self.create_map_text(dataset_name);
                        }
                        SiemDatasetType::CustomMapTextList(dataset_name) => {
                            self.create_map_text_list(dataset_name);
                        }
                        SiemDatasetType::CustomIpList(dataset_name) => {
                            self.create_map_ip_list(dataset_name);
                        }
                        SiemDatasetType::CustomIpMap(dataset_name) => {
                            self.create_map_ip(dataset_name);
                        }
                        SiemDatasetType::CustomIpList(dataset_name) => {
                            self.create_map_ip_list(dataset_name);
                        }
                        SiemDatasetType::CustomMapIpNet(dataset_name) => {
                            self.create_map_ip_net(dataset_name);
                        }
                        SiemDatasetType::CustomTextList(dataset_name) => {
                            self.create_text_list(dataset_name);
                            match dataset_text_list(&self.conn, dataset_name) {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::CustomTextList(dataset_name.clone()),
                                            SiemDataset::CustomTextList((
                                                Cow::Owned(dataset_name.to_string()),
                                                TextSetSynDataset::new(Arc::from(d), s.clone()),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create CustomTextList"),
                            };
                        }
                        SiemDatasetType::Secrets(dataset_name) => {
                            self.create_text_list(dataset_name);
                            match dataset_text_list(&self.conn, dataset_name) {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::CustomTextList(dataset_name.clone()),
                                            SiemDataset::CustomTextList((
                                                Cow::Owned(dataset_name.to_string()),
                                                TextSetSynDataset::new(Arc::from(d), s.clone()),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create CustomTextList"),
                            };
                        }
                        SiemDatasetType::AssetTag => {
                            self.create_map_text_list("AssetTag");
                        }
                        SiemDatasetType::BlockCountry => {
                            self.create_map_text_list("BlockCountry");
                            match dataset_text_list(&self.conn, "BlockCountry") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::BlockCountry,
                                            SiemDataset::BlockCountry(TextSetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create BlockCountry"),
                            }
                        }
                        SiemDatasetType::BlockDomain => {
                            self.create_map_text_list("BlockDomain");
                            match dataset_text_list(&self.conn, "BlockDomain") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::BlockDomain,
                                            SiemDataset::BlockDomain(TextSetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create BlockDomain"),
                            }
                        }
                        SiemDatasetType::BlockEmailSender => {
                            self.create_map_text_list("BlockEmailSender");
                            match dataset_text_list(&self.conn, "BlockEmailSender") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::BlockEmailSender,
                                            SiemDataset::BlockEmailSender(TextSetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create BlockEmailSender"),
                            }
                        }
                        SiemDatasetType::BlockIp => {
                            self.create_ip_set("BlockIp");
                            match dataset_ip_set(&self.conn, "BlockIp") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateIpSet(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::BlockIp,
                                            SiemDataset::BlockIp(IpSetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create BlockIp"),
                            }
                        }
                        SiemDatasetType::Configuration => {
                            self.create_map_text("Configuration");
                            match dataset_map_text(&self.conn, "Configuration") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::Configuration,
                                            SiemDataset::Configuration(TextMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create Configuration"),
                            }
                        }
                        SiemDatasetType::GeoIp => {
                            self.create_map_ip_net("GeoIp");
                        }
                        SiemDatasetType::HostUser => {
                            self.create_map_text("HostUser");
                            match dataset_map_text(&self.conn, "HostUser") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::HostUser,
                                            SiemDataset::HostUser(TextMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create HostUser"),
                            }
                        }
                        SiemDatasetType::HostVulnerable => {
                            self.create_map_text_list("HostVulnerable");
                            match dataset_map_text_list(&self.conn, "HostVulnerable") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMapList(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::HostVulnerable,
                                            SiemDataset::HostVulnerable(
                                                TextMapListSynDataset::new(Arc::from(d), s.clone()),
                                            ),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create HostVulnerable"),
                            }
                        }
                        SiemDatasetType::IpCloudProvider => {
                            self.create_map_ip_net("IpCloudProvider");
                            match self.dataset_map_ip_net("IpCloudProvider") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateNetIp(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::IpCloudProvider,
                                            SiemDataset::IpCloudProvider(IpNetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create IpCloudProvider"),
                            }
                        }
                        SiemDatasetType::IpCloudService => {
                            self.create_map_ip_net("IpCloudService");
                            match self.dataset_map_ip_net("IpCloudService") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateNetIp(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::IpCloudService,
                                            SiemDataset::IpCloudService(IpNetSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create IpCloudService"),
                            }
                        }
                        SiemDatasetType::IpDNS => {
                            self.create_map_ip_list("IpDNS");
                        }
                        SiemDatasetType::IpHeadquarters => {
                            self.create_map_ip_net("IpHeadquarters");
                        }
                        SiemDatasetType::IpHost => {
                            self.create_map_ip("IpHost");
                            match dataset_map_ip(&self.conn, "IpHost") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateIpMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::IpHost,
                                            SiemDataset::IpHost(IpMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create IpHost"),
                            }
                        }
                        SiemDatasetType::IpMac => {
                            self.create_map_ip("IpMac");
                            match dataset_map_ip(&self.conn, "IpMac") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateIpMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::IpMac,
                                            SiemDataset::IpMac(IpMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create IpMac"),
                            }
                        }
                        SiemDatasetType::MacHost => {
                            self.create_map_text("MacHost");
                            match dataset_map_text(&self.conn, "MacHost") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::MacHost,
                                            SiemDataset::MacHost(TextMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create MacHost"),
                            }
                        }
                        SiemDatasetType::UserHeadquarters => {
                            self.create_map_text("UserHeadquarters");
                            match dataset_map_text(&self.conn, "UserHeadquarters") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMap(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::UserHeadquarters,
                                            SiemDataset::UserHeadquarters(TextMapSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create UserHeadquarters"),
                            }
                        }
                        SiemDatasetType::UserTag => {
                            self.create_map_text_list("UserTag");
                            match dataset_map_text_list(&self.conn, "UserTag") {
                                Ok(d) => match listener {
                                    UpdateListener::UpdateTextMapList(s, _r, _t) => {
                                        datasets.insert(
                                            SiemDatasetType::UserTag,
                                            SiemDataset::UserTag(TextMapListSynDataset::new(
                                                Arc::from(d),
                                                s.clone(),
                                            )),
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => panic!("Cannot create UserTag"),
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(_) => {
                panic!("Cannot lock DATASET object");
            }
        }

        // Return datasets
        return Arc::clone(&DATASETS);
    }
    fn set_dataset_channels(
        &mut self,
        channels: Arc<Mutex<BTreeMap<SiemDatasetType, Vec<Sender<SiemMessage>>>>>,
    ) {
        self.comp_channels = channels;
    }

    fn register_dataset(&mut self, dataset: SiemDatasetType) {
        let time = chrono::Utc::now().timestamp_millis();
        if !self.registered_datasets.contains_key(&dataset) {
            let listener: UpdateListener = match &dataset {
                SiemDatasetType::CustomMapText(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::CustomIpList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::CustomMapIpNet(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::CustomIpMap(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::CustomMapTextList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::CustomTextList(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::Secrets(name) => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::GeoIp => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateGeoIp(channel.0, channel.1, time)
                }
                SiemDatasetType::IpHost => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateIpMap(channel.0, channel.1, time)
                }
                SiemDatasetType::IpMac => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateIpMap(channel.0, channel.1, time)
                }
                SiemDatasetType::IpDNS => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateIpMapList(channel.0, channel.1, time)
                }
                SiemDatasetType::MacHost => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::HostUser => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::BlockIp => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateIpSet(channel.0, channel.1, time)
                }
                SiemDatasetType::BlockDomain => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextSet(channel.0, channel.1, time)
                }
                SiemDatasetType::BlockEmailSender => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextSet(channel.0, channel.1, time)
                }
                SiemDatasetType::BlockCountry => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextSet(channel.0, channel.1, time)
                }
                SiemDatasetType::HostVulnerable => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMapList(channel.0, channel.1, time)
                }
                SiemDatasetType::UserTag => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMapList(channel.0, channel.1, time)
                }
                SiemDatasetType::AssetTag => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMapList(channel.0, channel.1, time)
                }
                SiemDatasetType::IpCloudService => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateNetIp(channel.0, channel.1, time)
                }
                SiemDatasetType::IpCloudProvider => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateNetIp(channel.0, channel.1, time)
                }
                SiemDatasetType::UserHeadquarters => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                SiemDatasetType::IpHeadquarters => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateNetIp(channel.0, channel.1, time)
                }
                SiemDatasetType::Configuration => {
                    let channel = crossbeam_channel::bounded(128);
                    UpdateListener::UpdateTextMap(channel.0, channel.1, time)
                }
                _ => {
                    println!("Dataset type not defined!!!");
                    return;
                }
            };
            self.registered_datasets.insert(dataset, listener);
        }
    }
}

fn ip_form_vec8(v: &Vec<u8>) -> Result<SiemIp, ()> {
    if v.len() == 4 {
        return Ok(SiemIp::V4(
            ((v[0] as u32) << 24) | ((v[1] as u32) << 16) | ((v[2] as u32) << 8) | (v[3] as u32),
        ));
    } else if v.len() == 16 {
        return Ok(SiemIp::V6(
            ((v[0] as u128) << 120)
                | ((v[1] as u128) << 112)
                | ((v[2] as u128) << 104)
                | ((v[3] as u128) << 96)
                | ((v[4] as u128) << 88)
                | ((v[5] as u128) << 80)
                | ((v[6] as u128) << 72)
                | ((v[7] as u128) << 64)
                | ((v[8] as u128) << 56)
                | ((v[9] as u128) << 48)
                | ((v[10] as u128) << 40)
                | ((v[11] as u128) << 32)
                | ((v[12] as u128) << 24)
                | ((v[13] as u128) << 16)
                | ((v[14] as u128) << 8)
                | (v[15] as u128),
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
fn dataset_map_ip(conn: &Connection, name: &str) -> rusqlite::Result<IpMapDataset> {
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
fn dataset_map_text(conn: &Connection, name: &str) -> rusqlite::Result<TextMapDataset> {
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
    use super::*;

    #[test]
    fn test_kernel_instance() {
        let mut manager = match SqliteDatasetManager::debug() {
            Ok(manager) => manager,
            Err(_) => {
                panic!("Cannot initialize DatasetManager")
            }
        };
        manager.register_dataset(SiemDatasetType::IpMac);
        let dt = manager.get_datasets();
        let mut ip_mac = match dt.lock() {
            Ok(datasets) => match datasets.get(&SiemDatasetType::IpMac) {
                Some(val) => match val {
                    SiemDataset::IpMac(syn_dataset) => syn_dataset.clone(),
                    _ => {
                        panic!("Dataset is not SiemDataset::IpMac")
                    }
                },
                None => {
                    panic!("No datasets availables")
                }
            },
            Err(_) => {
                panic!("Cannot access datasets Mutex")
            }
        };
        let _ = ip_mac.insert(SiemIp::V4(2020), Cow::Borrowed("default_ip"));
        manager.run()
    }
}
