use noria::ControllerHandle;
use std::io::Write;

#[tokio::main]
async fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE User(name varchar(255), dob varchar(255)) USER_COLUMN = name;
               CREATE TABLE Visit(name varchar(255), url varchar(255)) USER_COLUMN = name;
               QUERY VisitedWebsites: SELECT Visit.url FROM Visit WHERE Visit.name = ?;
               ";
    let user = "Alice";
    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/receiver")
        .await
        .unwrap();
    srv.install_recipe(sql).await.unwrap();

    let mut view = srv.view("VisitedWebsites").await.unwrap();
    let file = std::env::args().nth(1).expect("The exported file expected");
    let armored = std::fs::read_to_string(&file).expect("failed reading file");

    srv.import_user_shard(
        &armored,
        "test.receiver@example.com",
        "test.sender@example.com",
        |file| {
            file.write_all(b"123456\n").unwrap();
        }
    ).await.unwrap();

    let visits = view.lookup(&[user.into()], true).await.unwrap();
    println!("visits: {:#?}", visits);

}
