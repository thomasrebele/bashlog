package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import bashlog.plan.SortNode;
import common.plan.node.PlanNode;

/** Translates a sort node to a sort command */
public class Sort implements Translator {

	@Override
	public Bash translate(PlanNode p, CompilerInternals bc) {
		SortNode s = (SortNode)p;
		int[] cols = s.sortColumns();
		Bash prev = bc.compile(s.getTable());
		Bash.Pipe result = new Bash.Pipe(prev);
		Bash.Command cmd = result.cmd("$sort").arg("-t $'\\t'");

		boolean supportsUniq = cols == null;
		if (cols != null) {
			int used[] = new int[s.getTable().getArity()];
			Arrays.fill(used, 0);
			for (int col : cols) {
				cmd.arg("-k " + (col + 1));
				used[col] = 1;
			}
			if (Arrays.stream(used).allMatch(k -> k == 1)) {
				supportsUniq = true;
			}
		}
		if (supportsUniq) {
			cmd.arg("-u");
		}
		cmd.file(prev);

		return cmd;
	}

	@Override
	public List<Class<?>> supports() {
		return Arrays.asList(SortNode.class);
	}
}
