//tslint:disable interface-name no-empty-interface variable-name no-any
declare module "memdown" {
    import {LevelDown as LevelDOWN} from "leveldown";

    export interface MemDown<K = any, V = any> extends LevelDOWN {}

    interface MemDownConstructor {
    new <K = any, V = any>(): MemDown<K, V>;
    <K = any, V = any>(): MemDown<K, V>;
    }

    const MemDown: MemDownConstructor;
    export default MemDown;
}
//tslint:enable
