import setGlobal from 'a5base/setGlobal'

const g = setGlobal()

export default class Fuente {

  static async hasAccess(user_id : number, fuentes_id : number, write : boolean=false) : Promise<boolean> {
    var q = `SELECT EXISTS (
      SELECT 1 
      FROM user_fuentes_access 
      WHERE user_id=$1 
      AND fuentes_id=$2 
      ${(write) ? "AND effective_access='write'" : ""})`
    const result = await (g.pool as any).query(q, [user_id, fuentes_id])
    if(result.rows.length && result.rows[0].exists) {
			return true
		} else {
			return false
		}
  }

}