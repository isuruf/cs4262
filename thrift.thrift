
struct Node {
    1: string ip,
    2: i16 port,
    3: string name = "",
}

service dist {
    i16 join(1:Node me),
    i16 leave(1:Node me),
    oneway void search(1:string filename, 2:Node requestor, 3:i16 hops, 4:string uuid),
    oneway void found_file(1:list<string> files, 2:Node n, 4:string uuid),
}
