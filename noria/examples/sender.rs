use noria::ControllerHandle;
use std::io::Write;

#[tokio::main]
async fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE User(name varchar(255), dob varchar(255)) UNDELETABLE USER_COLUMN = name;
               CREATE TABLE Visit(name varchar(255), url varchar(255)) USER_COLUMN = name;
               QUERY VisitedWebsites: SELECT Visit.url FROM Visit WHERE Visit.name = ?;
               QUERY DateOfBirth: SELECT User.dob FROM User WHERE User.name = ?;
               ";
    let user1 = "Alice";
    let user2 = "Bob";
    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/sender")
        .await
        .unwrap();
    srv.install_recipe(sql).await.unwrap();

    let mut view = srv.view("VisitedWebsites").await.unwrap();
    if view.lookup(&[user2.into()], true).await.unwrap().is_empty() {
        let mut users = srv.table("User").await.unwrap();
        users.insert(vec![user1.into(), "1996-01-01".into()]).await.unwrap();
        users.insert(vec![user2.into(), "1996-04-01".into()]).await.unwrap();

        let mut visits = srv.table("Visit").await.unwrap();
        visits.insert(vec![user1.into(), "www.google.com".into()]).await.unwrap();
        visits.insert(vec![user1.into(), "www.facebook.com".into()]).await.unwrap();
        visits.insert(vec![user2.into(), "www.brown.edu".into()]).await.unwrap();
    }

    let armored = srv.export_user_shard(
        String::from(user1),
        "test.sender@example.com",
        "test.receiver@example.com",
        |file| {
            file.write_all(b"123456\n").unwrap();
        }
    ).await.unwrap();

    println!("{}", armored);

    let visits = view.lookup(&[user1.into()], true).await.unwrap();
    assert!(visits.is_empty());

    let mut tbl = srv.table("User").await.unwrap();
    tbl.delete(vec![user2.into(), "1996-04-01".into()]).await.unwrap();
    let mut dob_view = srv.view("DateOfBirth").await.unwrap();
    let dobs = dob_view.lookup(&[user2.into()], true).await.unwrap();
    eprintln!("{:#?}", dobs);
}
