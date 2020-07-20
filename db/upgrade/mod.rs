// This file is part of Moonfire NVR, a security camera network video recorder.
// Copyright (C) 2016 The Moonfire NVR Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// In addition, as a special exception, the copyright holders give
// permission to link the code of portions of this program with the
// OpenSSL library under certain conditions as described in each
// individual source file, and distribute linked combinations including
// the two.
//
// You must obey the GNU General Public License in all respects for all
// of the code used other than OpenSSL. If you modify file(s) with this
// exception, you may extend this exception to your version of the
// file(s), but you are not obligated to do so. If you do not wish to do
// so, delete this exception statement from your version. If you delete
// this exception statement from all source files in the program, then
// also delete it here.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

/// Upgrades the database schema.
///
/// See `guide/schema.md` for more information.
use crate::db;
use failure::{bail, Error};
use log::info;
use nix::NixPath;
use rusqlite::params;
use std::ffi::CStr;
use std::io::Write;
use uuid::Uuid;

mod v0_to_v1;
mod v1_to_v2;
mod v2_to_v3;
mod v3_to_v4;
mod v4_to_v5;

const UPGRADE_NOTES: &'static str =
    concat!("upgraded using moonfire-db ", env!("CARGO_PKG_VERSION"));

#[derive(Debug)]
pub struct Args<'a> {
    pub sample_file_dir: Option<&'a std::path::Path>,
    pub preset_journal: &'a str,
    pub no_vacuum: bool,
}

fn set_journal_mode(conn: &rusqlite::Connection, requested: &str) -> Result<(), Error> {
    assert!(!requested.contains(';')); // quick check for accidental sql injection.
    let actual = conn.query_row(
        &format!("pragma journal_mode = {}", requested),
        params![],
        |row| row.get::<_, String>(0),
    )?;
    info!(
        "...database now in journal_mode {} (requested {}).",
        actual, requested
    );
    Ok(())
}

fn upgrade(args: &Args, target_ver: i32, conn: &mut rusqlite::Connection) -> Result<(), Error> {
    let upgraders = [
        v0_to_v1::run,
        v1_to_v2::run,
        v2_to_v3::run,
        v3_to_v4::run,
        v4_to_v5::run,
    ];

    {
        assert_eq!(upgraders.len(), db::EXPECTED_VERSION as usize);
        let old_ver = conn.query_row("select max(id) from version", params![], |row| row.get(0))?;
        if old_ver > db::EXPECTED_VERSION {
            bail!(
                "Database is at version {}, later than expected {}",
                old_ver,
                db::EXPECTED_VERSION
            );
        } else if old_ver < 0 {
            bail!("Database is at negative version {}!", old_ver);
        }
        info!(
            "Upgrading database from version {} to version {}...",
            old_ver, target_ver
        );
        set_journal_mode(&conn, args.preset_journal)?;
        for ver in old_ver..target_ver {
            info!("...from version {} to version {}", ver, ver + 1);
            let tx = conn.transaction()?;
            upgraders[ver as usize](&args, &tx)?;
            tx.execute(
                r#"
                insert into version (id, unix_time, notes)
                             values (?, cast(strftime('%s', 'now') as int32), ?)
            "#,
                params![ver + 1, UPGRADE_NOTES],
            )?;
            tx.commit()?;
        }
    }

    Ok(())
}

pub fn run(args: &Args, conn: &mut rusqlite::Connection) -> Result<(), Error> {
    db::set_integrity_pragmas(conn)?;
    upgrade(args, db::EXPECTED_VERSION, conn)?;

    // WAL is the preferred journal mode for normal operation; it reduces the number of syncs
    // without compromising safety.
    set_journal_mode(&conn, "wal")?;
    if !args.no_vacuum {
        info!("...vacuuming database after upgrade.");
        conn.execute_batch(
            r#"
            pragma page_size = 16384;
            vacuum;
        "#,
        )?;
    }
    info!("...done.");

    Ok(())
}

/// A uuid-based path, as used in version 0 and version 1 schemas.
struct UuidPath([u8; 37]);

impl UuidPath {
    pub(crate) fn from(uuid: Uuid) -> Self {
        let mut buf = [0u8; 37];
        write!(&mut buf[..36], "{}", uuid.to_hyphenated_ref())
            .expect("can't format uuid to pathname buf");
        UuidPath(buf)
    }
}

impl NixPath for UuidPath {
    fn is_empty(&self) -> bool {
        false
    }
    fn len(&self) -> usize {
        36
    }

    fn with_nix_path<T, F>(&self, f: F) -> Result<T, nix::Error>
    where
        F: FnOnce(&CStr) -> T,
    {
        let p = CStr::from_bytes_with_nul(&self.0[..]).expect("no interior nuls");
        Ok(f(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compare;
    use crate::testutil;
    use failure::ResultExt;

    fn new_conn() -> Result<rusqlite::Connection, Error> {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute("pragma foreign_keys = on", params![])?;
        conn.execute("pragma fullfsync = on", params![])?;
        conn.execute("pragma synchronous = 2", params![])?;
        Ok(conn)
    }

    fn compare(c: &rusqlite::Connection, ver: i32, fresh_sql: &str) -> Result<(), Error> {
        let fresh = new_conn()?;
        fresh.execute_batch(fresh_sql)?;
        if let Some(diffs) = compare::get_diffs("upgraded", &c, "fresh", &fresh)? {
            panic!("Version {}: differences found:\n{}", ver, diffs);
        }
        Ok(())
    }

    /// Upgrades and compares schemas.
    /// Doesn't (yet) compare any actual data.
    #[test]
    fn upgrade_and_compare() -> Result<(), Error> {
        testutil::init();
        let tmpdir = tempdir::TempDir::new("moonfire-nvr-test")?;
        //let path = tmpdir.path().to_str().ok_or_else(|| format_err!("invalid UTF-8"))?.to_owned();
        let mut upgraded = new_conn()?;
        upgraded.execute_batch(include_str!("v0.sql"))?;
        upgraded.execute_batch(
            r#"
            insert into camera (id, uuid, short_name, description, host, username, password,
                                main_rtsp_path, sub_rtsp_path, retain_bytes)
                        values (1, zeroblob(16), 'test camera', 'desc', 'host', 'user', 'pass',
                                'main', 'sub', 42);
        "#,
        )?;
        upgraded.execute(
            r#"
            insert into video_sample_entry (id, sha1, width, height, data)
                                    values (1, X'3BA3EDE1BD93B7BCB7AB5BD099C047701451B822',
                                            1920, 1080, ?);
        "#,
            params![testutil::TEST_VIDEO_SAMPLE_ENTRY_DATA],
        )?;
        upgraded.execute_batch(
            r#"
            insert into recording (id, camera_id, sample_file_bytes, start_time_90k, duration_90k,
                                   local_time_delta_90k, video_samples, video_sync_samples,
                                   video_sample_entry_id, sample_file_uuid, sample_file_sha1,
                                   video_index)
                           values (1, 1, 42, 140063580000000, 90000, 0, 1, 1, 1,
                                   X'E69D45E8CBA64DC1BA2ECB1585983A10', zeroblob(20), X'00');
            insert into reserved_sample_files values (X'51EF700C933E4197AAE4EE8161E94221', 0),
                                                     (X'E69D45E8CBA64DC1BA2ECB1585983A10', 1);
        "#,
        )?;
        let rec1 = tmpdir.path().join("e69d45e8-cba6-4dc1-ba2e-cb1585983a10");
        let garbage = tmpdir.path().join("51ef700c-933e-4197-aae4-ee8161e94221");
        std::fs::File::create(&rec1)?;
        std::fs::File::create(&garbage)?;

        for (ver, fresh_sql) in &[
            (1, Some(include_str!("v1.sql"))),
            (2, None), // transitional; don't compare schemas.
            (3, Some(include_str!("v3.sql"))),
            (4, None), // transitional; don't compare schemas.
            (5, Some(include_str!("../schema.sql"))),
        ] {
            upgrade(
                &Args {
                    sample_file_dir: Some(&tmpdir.path()),
                    preset_journal: "delete",
                    no_vacuum: false,
                },
                *ver,
                &mut upgraded,
            )
            .context(format!("upgrading to version {}", ver))?;
            if let Some(f) = fresh_sql {
                compare(&upgraded, *ver, f)?;
            }
            if *ver == 3 {
                // Check that the garbage files is cleaned up properly, but also add it back
                // to simulate a bug prior to 433be217. The v5 upgrade should take care of
                // anything left over.
                assert!(!garbage.exists());
                std::fs::File::create(&garbage)?;
            }
        }

        // Check that recording files get renamed.
        assert!(!rec1.exists());
        assert!(tmpdir.path().join("0000000100000001").exists());

        // Check that garbage files get cleaned up.
        assert!(!garbage.exists());

        Ok(())
    }
}
